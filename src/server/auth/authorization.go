package auth

import (
	"net/http"
	"io"
	"encoding/json"
	"io/ioutil"
	"strings"
	"os"
	"bytes"
	"github.com/pkg/errors"
	"encoding/base64"
	"crypto/hmac"
	"crypto/sha1"
)

type AuthResponse struct {
	Status string   `json:"status"`
	User User       `json:"data"`
}

type User struct {
	Id 		int 	`json:"id"`
	Login 	string	`json:"login"`
	Status 	string	`json:"status"`
	Role 	string	`json:"role"`
	ABAC 	map[string]interface{}  `json:"abac"`
	Authorized bool `json:"authorized"`

	// @todo: remove LinkedObject field as deprecated at 26 Nov 2019
	LinkedObject map[string]interface{}  `json:"linked_object"`
}

func NewError(text string) error {
	return &AuthError{text}
}

type AuthError struct {
	s string
}

func (this *AuthError) Error () string {
	return this.s
}

func (this *AuthError) Serialize () map[string]string {
	return map[string]string{
		"code": "401",
		"msg":  this.s,
	}
}

type Authenticator interface {
	Authenticate(*http.Request) (*User, map[string]interface{}, error)
}

type EmptyAuthenticator struct {}

func (this *EmptyAuthenticator) Authenticate(req *http.Request) (*User, map[string]interface{}, error) {
	return &User{Authorized: false}, nil, nil
}

type TroodAuthenticator struct {
	AuthUrl string
}

func GetServiceToken() (string, error) {
	secret := os.Getenv("SERVICE_AUTH_SECRET")
	domain := os.Getenv("SERVICE_DOMAIN")

	if secret != "" && domain != "" {

		key := sha1.New()
		key.Write([]byte("trood.signer" + secret))

		signature := hmac.New(sha1.New, key.Sum(nil))
		signature.Write([]byte(domain))

		result := domain + ":" + base64.RawURLEncoding.EncodeToString(signature.Sum(nil))

		return result, nil
	}

	return "", errors.New("SERVICE_AUTH_SECRET or SERVICE_DOMAIN not found")
}

func (this *TroodAuthenticator) Authenticate(req *http.Request) (*User, map[string]interface{}, error){
	var auth_header = req.Header.Get("Authorization")

	if auth_header != "" {
		client := &http.Client{}

		user_token := strings.Split(auth_header, " ");
		service_token, err := GetServiceToken()

		token_type := "user"
		if user_token[0] == "Service" {
			token_type = "service"
		}

		body := []byte(`{"type":"`+token_type+`", "token":"`+user_token[1]+`"}`)

		auth_request, _ := http.NewRequest("POST", this.AuthUrl + "/api/v1.0/verify-token", bytes.NewBuffer(body))
		auth_request.Header.Add("Authorization", "Service " + service_token)
		auth_request.Header.Add("Content-Type", "application/json")

		auth_response, err  := client.Do(auth_request)

		if err == nil && auth_response.StatusCode == 200 {
			user, err := this.FetchUser(auth_response.Body)

			if err == nil {
				user.Authorized = true
				return user, user.ABAC, nil
			}

			return nil, nil, NewError("Cant achieve user object")
		}

		return nil, nil, NewError("Authorization failed")
	}

	if rules_response, err := http.Get(this.AuthUrl + "/api/v1.0/abac?domain=" + os.Getenv("SERVICE_DOMAIN")); err == nil {
		var rules map[string]interface{}
		body, _ := ioutil.ReadAll(rules_response.Body)
		err = json.Unmarshal(body, &rules)
		return &User{Authorized: false}, rules["data"].(map[string]interface{}), nil
	} else {
		return nil, nil, err
	}

}


func (this *TroodAuthenticator) FetchUser(buff io.ReadCloser) (*User, error)  {
	response := AuthResponse{}
	body, err := ioutil.ReadAll(buff)
	if err == nil {
		err = json.Unmarshal(body, &response)

		return &response.User, nil

	} else {
		return nil, err
	}
}
