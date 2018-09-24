package errors

import (
	"fmt"
	"encoding/json"
)

//Server errors description
const (
	ErrUnsupportedMediaType = "unsupported_media_type"
	ErrBadRequest           = "bad_request"
	ErrInternalServerError  = "internal_server_error"
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
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("Server error: Status = %d, Code = '%s', Msg = '%s'", e.Status, e.Code, e.Msg)
}

func (e *ServerError) Serialize() map[string]string {
	return serializeError(e.Code, e.Msg)
}

func (e *ServerError) Json() []byte {
	encodedData, _ := json.Marshal(e.Serialize())
	return encodedData
}

func serializeError(errorCode string, errorMessage string) map[string]string {
	return map[string]string{
		"Code": errorCode,
		"Msg":  errorMessage,
	}
}
