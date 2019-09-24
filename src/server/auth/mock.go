package auth

import "net/http"

type MockAuthenticator struct {
	user User
}

func (ma *MockAuthenticator) Authenticate(req *http.Request) (User, map[string]interface{}, error) {
	return ma.user, ma.user.ABAC, nil
}

func NewMockAuthenticator(user User) *MockAuthenticator {
	return &MockAuthenticator{user: user}
}
