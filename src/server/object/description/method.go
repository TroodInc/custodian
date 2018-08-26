package description

import "encoding/json"

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
		return nil, NewMetaDescriptionError("", "json_marshal", ErrJsonMarshal, "Incorrect method: %v", mt)
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
		return NewMetaDescriptionError("", "json_unmarshal", ErrJsonUnmarshal, "Incorrect method: %s", s)
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
