package notifications

import (
	"encoding/json"
	"fmt"
	"server/errors"
)

type Method int

const (
	MethodRetrieve Method = iota + 1
	MethodCreate
	MethodRemove
	MethodUpdate
)

func (m Method) AsString() (string) {
	switch m {
	case MethodRetrieve:
		return "retrieve"
	case MethodCreate:
		return "create"
	case MethodRemove:
		return "remove"
	case MethodUpdate:
		return "update"
	default:
		return ""
	}
}

func (m Method) Validate() (string, bool) {
	if methodAsString := m.AsString(); methodAsString == "" {
		return methodAsString, false
	} else {
		return methodAsString, true
	}
}

func (mt Method) MarshalJSON() ([]byte, error) {
	if s, ok := mt.Validate(); ok {
		return json.Marshal(s)
	} else {
		return nil, errors.NewValidationError("json_marshal", fmt.Sprintf("Incorrect method: %v", mt), nil)
	}
}

func (mt *Method) UnmarshalJSON(b []byte) error {
	var s string
	if e := json.Unmarshal(b, &s); e != nil {
		return e
	}
	if m, ok := AsMethod(s); ok {
		*mt = m
		return nil
	} else {
		return  errors.NewValidationError( "json_unmarshal", fmt.Sprintf("Incorrect method: %s", s), nil)
	}
}

func AsMethod(s string) (Method, bool) {
	switch s {
	case "retrieve":
		return MethodRetrieve, true
	case "create":
		return MethodCreate, true
	case "remove":
		return MethodRemove, true
	case "update":
		return MethodUpdate, true
	default:
		return 0, false
	}
}
