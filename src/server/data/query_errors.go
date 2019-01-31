package data

import (
	"encoding/json"
	"fmt"
)

const (
	ErrCodeFieldNotFound = "field_not_found"
)

type QueryError struct {
	Code string
	Msg  string
}

func (e *QueryError) Error() string {
	return fmt.Sprintf("Query error: %s", e.Msg)
}

func (e *QueryError) Json() []byte {
	encodedErr, _ := json.Marshal(map[string]string{
		"code": e.Code,
		"msg":  e.Msg,
	})
	return encodedErr
}

func NewQueryError(code string, msg string) *QueryError {
	return &QueryError{Code: code, Msg: msg}
}
