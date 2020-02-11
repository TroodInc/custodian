package object

import (
	"encoding/json"
	"errors"
)

//Types description
type FieldType int

const (
	FieldTypeString   FieldType = iota + 1
	FieldTypeNumber
	FieldTypeBool
	FieldTypeArray
	FieldTypeObject
	FieldTypeObjects
	FieldTypeDateTime
	FieldTypeDate
	FieldTypeTime
	FieldTypeGeneric
)

type FieldMode int

const (
	FieldModeRetrieve FieldMode = iota + 1
	FieldModeQuery
)

func AsFieldType(s string) FieldType {
	switch s {
	case "string":
		return FieldTypeString
	case "number":
		return FieldTypeNumber
	case "bool":
		return FieldTypeBool
	case "array":
		return FieldTypeArray
	case "object":
		return FieldTypeObject
	case "objects":
		return FieldTypeObjects
	case "generic":
		return FieldTypeGeneric
	case "datetime":
		return FieldTypeDateTime
	case "date":
		return FieldTypeDate
	case "time":
		return FieldTypeTime
	default:
		return 0
	}
}

func (fieldType FieldType) String() string {
	switch fieldType {
	case FieldTypeString:
		return "string"
	case FieldTypeNumber:
		return "number"
	case FieldTypeBool:
		return "bool"
	case FieldTypeArray:
		return "array"
	case FieldTypeObject:
		return "object"
	case FieldTypeObjects:
		return "objects"
	case FieldTypeDateTime:
		return "datetime"
	case FieldTypeDate:
		return "date"
	case FieldTypeTime:
		return "time"
	case FieldTypeGeneric:
		return "generic"
	default:
		return ""
	}
}

func (fieldType FieldType) DdlType() (string, error) {
	switch fieldType {
	case FieldTypeString:
		return "text", nil
	case FieldTypeNumber:
		return "numeric", nil
	case FieldTypeBool:
		return "bool", nil
	case FieldTypeDate:
		return "date", nil
	case FieldTypeDateTime:
		return "timestamp with time zone", nil
	case FieldTypeTime:
		return "time with time zone", nil
	default:
		return "", errors.New("Unsupported column type: " + string(fieldType))
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
	case FieldTypeObjects:
		_, ok := i.([]interface{})
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
	case FieldTypeObjects:
		return func(i interface{}) bool {
			_, ok := i.([]interface{})
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
	if assumedFieldType := AsFieldType(str); assumedFieldType > 0 {
		*fieldType = assumedFieldType
		return nil
	} else {
		return NewMetaDescriptionError("", "json_unmarshal", ErrJsonUnmarshal, "Incorrect NewField type: %str", str)
	}
}

func (fieldType FieldType) MarshalJSON() ([]byte, error) {
	if s := fieldType.String(); s != "" {
		return json.Marshal(s)
	} else {
		return nil, NewMetaDescriptionError("", "json_marshal", ErrJsonMarshal, "Incorrect field type: %v", fieldType)
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

func AsLinkType(s string) LinkType {
	switch s {
	case "outer":
		return LinkTypeOuter
	case "inner":
		return LinkTypeInner
	default:
		return 0
	}
}

type MetaNameList []string

func (mnl MetaNameList) Diff(anotherMetaNameList MetaNameList) []string {
	diff := make([]string, 0)
	for _, aMetaName := range mnl {
		metaNotFound := true
		for _, bMetaName := range anotherMetaNameList {
			if bMetaName == aMetaName {
				metaNotFound = false
			}
		}
		if metaNotFound {
			diff = append(diff, aMetaName)
		}
	}
	return diff
}

func (lt *LinkType) UnmarshalJSON(b []byte) error {
	var s string
	if e := json.Unmarshal(b, &s); e != nil {
		return e
	}
	if l := AsLinkType(s); l > 0 {
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

type Def interface{}
type DefConstStr struct{ Value string }
type DefConstFloat struct{ Value float64 }
type DefConstInt struct{ Value int }
type DefConstBool struct{ Value bool }
type DefExpr struct {
	Func string
	Args []interface{}
}
