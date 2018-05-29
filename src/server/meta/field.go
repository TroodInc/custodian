package meta

import (
	"encoding/json"
)

//Types description
type FieldType int

const (
	FieldTypeString FieldType = iota + 1
	FieldTypeNumber
	FieldTypeBool
	FieldTypeArray
	FieldTypeObject
	FieldTypeDateTime
	FieldTypeDate
)

func AsFieldType(s string) (FieldType, bool) {
	switch s {
	case "string":
		return FieldTypeString, true
	case "number":
		return FieldTypeNumber, true
	case "bool":
		return FieldTypeBool, true
	case "array":
		return FieldTypeArray, true
	case "object":
		return FieldTypeObject, true
	case "datetime":
		return FieldTypeDateTime, true
	case "date":
		return FieldTypeDate, true
	default:
		return 0, false
	}
}

func (fieldType FieldType) String() (string, bool) {
	switch fieldType {
	case FieldTypeString:
		return "string", true
	case FieldTypeNumber:
		return "number", true
	case FieldTypeBool:
		return "bool", true
	case FieldTypeArray:
		return "array", true
	case FieldTypeObject:
		return "object", true
	case FieldTypeDateTime:
		return "datetime", true
	case FieldTypeDate:
		return "date", true
	default:
		return "", false
	}
}

func (fieldType FieldType) AssertType(i interface{}) bool {
	switch fieldType {
	case FieldTypeString, FieldTypeDateTime, FieldTypeDate:
		_, ok := i.(string)
		return ok
	case FieldTypeNumber:
		_, ok := i.(float64)
		return ok
	case FieldTypeBool:
		_, ok := i.(bool)
		return ok
	case FieldTypeArray:
		_, ok := i.([]interface{})
		return ok
	case FieldTypeObject:
		_, ok := i.(map[string]interface{})
		return ok
	default:
		return false
	}
}

func (fieldType FieldType) TypeAsserter() func(interface{}) bool {
	switch fieldType {
	case FieldTypeString, FieldTypeDateTime, FieldTypeDate:
		return func(i interface{}) bool {
			_, ok := i.(string)
			return ok
		}
	case FieldTypeNumber:
		return func(i interface{}) bool {
			_, ok := i.(float64)
			return ok
		}
	case FieldTypeBool:
		return func(i interface{}) bool {
			_, ok := i.(bool)
			return ok
		}
	case FieldTypeArray:
		return func(i interface{}) bool {
			_, ok := i.([]interface{})
			return ok
		}
	case FieldTypeObject:
		return func(i interface{}) bool {
			_, ok := i.(map[string]interface{})
			return ok
		}
	default:
		return func(interface{}) bool { return false }
	}
}

func (fieldType *FieldType) UnmarshalJSON(b []byte) error {
	var str string
	if e := json.Unmarshal(b, &str); e != nil {
		return e
	}
	if assumedFieldType, ok := AsFieldType(str); ok {
		*fieldType = assumedFieldType
		return nil
	} else {
		return NewMetaError("", "json_unmarshal", ErrJsonUnmarshal, "Incorrect Field type: %str", str)
	}
}

func (fieldType FieldType) MarshalJSON() ([]byte, error) {
	if s, ok := fieldType.String(); ok {
		return json.Marshal(s)
	} else {
		return nil, NewMetaError("", "json_marshal", ErrJsonMarshal, "Incorrect filed type: %v", fieldType)
	}
}

//FieldDescription description.
type Field struct {
	Name           string      `json:"name"`
	Type           FieldType   `json:"type"`
	LinkMeta       string      `json:"linkMeta,omitempty"` //only for array and objects
	LinkType       LinkType    `json:"linkType,omitempty"`
	OuterLinkField string      `json:"outerLinkField,omitempty"`
	Optional       bool        `json:"optional"`
	Def            interface{} `json:"default,omitempty"`
}

func (f *Field) IsSimple() bool {
	return f.Type != FieldTypeObject && f.Type != FieldTypeArray
}
