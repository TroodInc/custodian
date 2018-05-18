package auth

import (
	"net/http"
	"io"
	"encoding/json"
	"io/ioutil"
)

type User struct {
	id int
	login string
	status string
	role string
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
		"code": "403",
		"msg":  this.s,
	}
}

type Authenticator interface {
	Authenticate(*http.Request) (User, error)
}

type EmptyAuthenticator struct {}

func (this *EmptyAuthenticator) Authenticate(req *http.Request) (User, error) {
	return User{}, nil
}

type TroodAuthenticator struct {
	AuthUrl string
}

func (this *TroodAuthenticator) Authenticate(req *http.Request) (User, error){
	var auth_header = req.Header.Get("Authorization")

	if auth_header != "" {
		client := &http.Client{}

		auth_request, _ := http.NewRequest("POST", this.AuthUrl + "/api/v1.0/verify-token", nil)
		auth_request.Header.Add("Authorization", auth_header)
		auth_response, err  := client.Do(auth_request)

		if err == nil && auth_response.StatusCode == 200 {
			user, err := this.FetchUser(auth_response.Body)

			if err == nil {
				return user, nil
			}

			return User{}, NewError("Cant achieve user object")
		}

		return User{}, NewError("Authorization failed")
	}

	return User{}, NewError("No Authorization header found")
}


func (this *TroodAuthenticator) FetchUser(buff io.ReadCloser) (User, error)  {
	user := User{}
	body, err := ioutil.ReadAll(buff)
	if err == nil {
		err = json.Unmarshal(body, &user)



		return user, nil

	} else {
		return User{}, err
	}
}
