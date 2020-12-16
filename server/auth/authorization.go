package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

type AuthResponse struct {
	Status string `json:"status"`
	User   User   `json:"data"`
}

type User struct {
	Id         int                    `json:"id"`
	Login      string                 `json:"login"`
	Status     string                 `json:"status"`
	Language   string                 `json:"language"`
	Role       map[string]interface{} `json:"role"`
	Type       string                 `json:"type"`
	ABAC       map[string]interface{} `json:"abac"`
	Authorized bool                   `json:"authorized"`
	Profile    map[string]interface{} `json:"profile"`
}

func NewError(text string) error {
	return &AuthError{text}
}

type AuthError struct {
	s string
}

func (this *AuthError) Error() string {
	return this.s
}

func (this *AuthError) Serialize() map[string]string {
	return map[string]string{
		"code": "401",
		"msg":  this.s,
	}
}

func GetAuthenticator() Authenticator {
	auth_type := os.Getenv("AUTHENTICATION_TYPE")

	switch auth_type {
	case "TROOD":
		service_url := os.Getenv("TROOD_AUTH_SERVICE_URL")

		cache_type := os.Getenv("CACHE_TYPE")
		redis_url := os.Getenv("REDIS_URL")
		if cache_type == "REDIS" && redis_url != "" {
			redis_options, _ := redis.ParseURL(redis_url)
			cache_client := redis.NewClient(redis_options)

			return &TroodAuthenticator{service_url, cache_client}
		}

		return &TroodAuthenticator{service_url, nil}
	default:
		return &EmptyAuthenticator{}
	}
}

type Authenticator interface {
	Authenticate(*http.Request) (*User, map[string]interface{}, error)
}

type EmptyAuthenticator struct{}

func (eauth *EmptyAuthenticator) Authenticate(req *http.Request) (*User, map[string]interface{}, error) {
	return &User{Authorized: false}, nil, nil
}

type TroodAuthenticator struct {
	AuthUrl string
	cache   *redis.Client
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

func (tauth *TroodAuthenticator) Authenticate(req *http.Request) (*User, map[string]interface{}, error) {
	var auth_header = req.Header.Get("Authorization")

	if auth_header != "" {

		if user, err := tauth.getUserFromCache(auth_header); err == nil {
			return user, user.ABAC, nil
		}

		if user, err := tauth.getUserFromAuthService(auth_header); err == nil {
			return user, user.ABAC, nil
		} else {
			return nil, nil, NewError("Authorization failed")
		}
	}

	if rules_response, err := http.Get(tauth.AuthUrl + "/api/v1.0/abac?domain=" + os.Getenv("SERVICE_DOMAIN")); err == nil {
		var rules map[string]interface{}
		body, _ := ioutil.ReadAll(rules_response.Body)
		err = json.Unmarshal(body, &rules)
		return &User{Authorized: false}, rules["data"].(map[string]interface{}), nil
	} else {
		return nil, nil, err
	}

}

func (tauth *TroodAuthenticator) getUserFromCache(token string) (*User, error) {
	if tauth.cache != nil {
		token_parts := strings.Split(token, " ")

		data, err := tauth.cache.Get(tauth.cache.Context(), "AUTH:"+token_parts[1]).Result()
		if err == nil {
			var user User
			err := json.Unmarshal([]byte(data), &user)
			user.Authorized = true

			return &user, err
		}

		return nil, err
	}

	return nil, NewError("Cache is not enabled")
}

func (tauth *TroodAuthenticator) getUserFromAuthService(token string) (*User, error) {
	user_token := strings.Split(token, " ")
	service_token, err := GetServiceToken()

	token_type := "user"
	if user_token[0] == "Service" {
		token_type = "service"
	}

	body := []byte(`{"type":"` + token_type + `", "token":"` + user_token[1] + `"}`)

	auth_request, _ := http.NewRequest("POST", tauth.AuthUrl+"/api/v1.0/verify-token/", bytes.NewBuffer(body))
	auth_request.Header.Add("Authorization", "Service "+service_token)
	auth_request.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	auth_response, err := client.Do(auth_request)

	if err == nil && auth_response.StatusCode == 200 {
		user, err := tauth.FetchUser(auth_response.Body)

		if err == nil {
			user.Authorized = true
			return user, nil
		}
	}

	return nil, NewError("Cant achieve user object")
}

func (tauth *TroodAuthenticator) FetchUser(buff io.ReadCloser) (*User, error) {
	response := AuthResponse{}
	body, err := ioutil.ReadAll(buff)
	if err == nil {
		err = json.Unmarshal(body, &response)

		return &response.User, nil

	} else {
		return nil, err
	}
}
