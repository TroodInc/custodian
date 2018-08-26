package description

import (
	"encoding/json"
)

//Types description
type FieldType int

const (
	FieldTypeString   FieldType = iota + 1
	FieldTypeNumber
	FieldTypeBool
	FieldTypeArray
	FieldTypeObject
	FieldTypeDateTime
	FieldTypeDate
	FieldTypeTime
	FieldTypeGeneric
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
	case "generic":
		return FieldTypeGeneric, true
	case "datetime":
		return FieldTypeDateTime, true
	case "date":
		return FieldTypeDate, true
	case "time":
		return FieldTypeTime, true
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
	case FieldTypeTime:
		return "time", true
	case FieldTypeGeneric:
		return "generic", true
	default:
		return "", false
	}
}

func (fieldType FieldType) AssertType(i interface{}) bool {
	switch fieldType {
	case FieldTypeString, FieldTypeDateTime, FieldTypeDate, FieldTypeTime:
		_, ok := i.(string)
		return ok
	case FieldTypeNumber:
		_, floatOk := i.(float64)
		_, intOk := i.(int)
		return floatOk || intOk
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
	case FieldTypeString, FieldTypeDateTime, FieldTypeDate, FieldTypeTime:
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
		return NewMetaDescriptionError("", "json_unmarshal", ErrJsonUnmarshal, "Incorrect Field type: %str", str)
	}
}

func (fieldType FieldType) MarshalJSON() ([]byte, error) {
	if s, ok := fieldType.String(); ok {
		return json.Marshal(s)
	} else {
		return nil, NewMetaDescriptionError("", "json_marshal", ErrJsonMarshal, "Incorrect filed type: %v", fieldType)
	}
}

type LinkType int

const (
	LinkTypeOuter LinkType = iota + 1 //Child refers to the parent
	LinkTypeInner                     //Parent refres to the Child
)

func (lt LinkType) String() (string, bool) {
	switch lt {
	case LinkTypeOuter:
		return "outer", true
	case LinkTypeInner:
		return "inner", true
	default:
		return "", false
	}
}

func AsLinkType(s string) (LinkType, bool) {
	switch s {
	case "outer":
		return LinkTypeOuter, true
	case "inner":
		return LinkTypeInner, true
	default:
		return 0, false
	}
}

func (lt *LinkType) UnmarshalJSON(b []byte) error {
	var s string
	if e := json.Unmarshal(b, &s); e != nil {
		return e
	}
	if l, ok := AsLinkType(s); ok {
		*lt = l
		return nil
	} else {
		return NewMetaDescriptionError("", "json_unmarshal", ErrJsonUnmarshal, "Incorrect link type: %s", s)
	}
}

func (lt LinkType) MarshalJSON() ([]byte, error) {
	if s, ok := lt.String(); ok {
		return json.Marshal(s)
	} else {
		return nil, NewMetaDescriptionError("", "json_marshal", ErrJsonMarshal, "Incorrect link type: %v", lt)
	}
}

//FieldDescription description.
type Field struct {
	Name           string      `json:"name"`
	Type           FieldType   `json:"type"`
	LinkMeta       string      `json:"linkMeta,omitempty"`     //only for array and objects
	LinkMetaList   []string    `json:"linkMetaList,omitempty"` //only for array and objects
	LinkType       LinkType    `json:"linkType,omitempty"`
	OuterLinkField string      `json:"outerLinkField,omitempty"`
	Optional       bool        `json:"optional"`
	OnDelete       string      `json:"onDelete,omitempty"`
	Def            interface{} `json:"default,omitempty"`
}

func (f *Field) IsSimple() bool {
	return f.Type != FieldTypeObject && f.Type != FieldTypeArray && f.Type != FieldTypeGeneric
}
