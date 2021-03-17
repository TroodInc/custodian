package errors

import (
	"encoding/json"
	"fmt"
	"net/http"
)

//Server errors description
const (
	ErrBadRequest           = "bad_request"
	ErrNotFound             = "not_found"
)

//The interface of error convertable to JSON in format {"Code":"some_code"; "Msg":"message"}.
type JsonError interface {
	Json() []byte
	Serialize() map[string]string
}

type ServerError struct {
	Status int
	Code   string
	Msg    string
	Data   interface{}
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("Server error: Status = %d, Code = '%s', Msg = '%s'", e.Status, e.Code, e.Msg)
}

func (e *ServerError) Serialize() map[string]interface{} {
	return map[string]interface{}{
		"Code": e.Code,
		"Msg":  e.Msg,
		"Data": e.Data,
	}
}

func (e *ServerError) Json() []byte {
	encodedData, _ := json.Marshal(e.Serialize())
	return encodedData
}


func NewFatalError(code string, msg string, data interface{}) *ServerError {
	return &ServerError{http.StatusInternalServerError, code, msg, data}
}

func NewValidationError(code string, msg string, data interface{}) *ServerError {
	return &ServerError{http.StatusBadRequest, code, msg, data}
}

func NewNotFoundError(code string, msg string, data interface{}) *ServerError {
	return &ServerError{http.StatusNotFound, code, msg, data}
}