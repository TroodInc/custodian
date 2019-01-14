package auth

import "net/http"

type MockAuthenticator struct {
	user User
}

func (ma *MockAuthenticator) Authenticate(req *http.Request) (User, error) {
	return ma.user, nil
}

func NewMockAuthenticator(user User) *MockAuthenticator {
	return &MockAuthenticator{user: user}
}
