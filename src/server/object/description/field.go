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
	case "objects":
		return FieldTypeObjects, true
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
	case FieldTypeObjects:
		return "objects", true
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
	if assumedFieldType, ok := AsFieldType(str); ok {
		*fieldType = assumedFieldType
		return nil
	} else {
		return NewMetaDescriptionError("", "json_unmarshal", ErrJsonUnmarshal, "Incorrect NewField type: %str", str)
	}
}

func (fieldType FieldType) MarshalJSON() ([]byte, error) {
	if s, ok := fieldType.String(); ok {
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

type Def interface{}
type DefConstStr struct{ Value string }
type DefConstFloat struct{ Value float64 }
type DefConstInt struct{ Value int }
type DefConstBool struct{ Value bool }
type DefExpr struct {
	Func string
	Args []interface{}
}

//FieldDescription description.
type Field struct {
	Name           string       `json:"name"`
	Type           FieldType    `json:"type"`
	LinkMeta       string       `json:"linkMeta,omitempty"`     //only for array and "object"
	LinkMetaList   MetaNameList `json:"linkMetaList,omitempty"` //only for array and "object"
	LinkType       LinkType     `json:"linkType,omitempty"`
	OuterLinkField string       `json:"outerLinkField,omitempty"`
	Optional       bool         `json:"optional"`
	OnDelete       string       `json:"onDelete,omitempty"`
	Def            interface{}  `json:"default,omitempty"`
	NowOnUpdate    bool         `json:"nowOnUpdate,omitempty"`
	NowOnCreate    bool         `json:"nowOnCreate,omitempty"`
	QueryMode      bool         `json:"queryMode,omitempty"`    //only for outer links, true if field should be used for querying
	RetrieveMode   bool         `json:"retrieveMode,omitempty"` //only for outer links, true if field should be used for data retrieving
	LinkThrough    string       `json:"linkThrough,omitempty"`  //only for "objects" field
}

func (f *Field) IsSimple() bool {
	return f.Type != FieldTypeObject && f.Type != FieldTypeArray && f.Type != FieldTypeGeneric && f.Type != FieldTypeObjects
}

func (f *Field) Default() Def {
	switch t := f.Def.(type) {
	case string:
		return DefConstStr{t}
	case float64:
		return DefConstFloat{t}
	case int:
		return DefConstInt{t}
	case bool:
		return DefConstBool{t}
	case map[string]interface{}:
		var args []interface{}
		if a, ok := t["args"]; ok {
			args = a.([]interface{})
		}
		return DefExpr{Func: t["func"].(string), Args: args}
	default:
		return nil
	}
}

func (f *Field) Clone() *Field {
	return &Field{
		Name:           f.Name,
		Type:           f.Type,
		LinkMeta:       f.LinkMeta,
		LinkMetaList:   f.LinkMetaList,
		LinkType:       f.LinkType,
		OuterLinkField: f.OuterLinkField,
		Optional:       f.Optional,
		OnDelete:       f.OnDelete,
		Def:            f.Def,
		QueryMode:      f.QueryMode,
		RetrieveMode:   f.RetrieveMode,
		LinkThrough:    f.LinkThrough,
	}
}

func (f *Field) OnDeleteStrategy() *OnDeleteStrategy {
	if f.Type == FieldTypeObject || (f.Type == FieldTypeGeneric && f.LinkType == LinkTypeInner) {
		onDeleteStrategy, err := GetOnDeleteStrategyByVerboseName(f.OnDelete)
		if err != nil {
			panic(err.Error())
		} else {
			return &onDeleteStrategy
		}
	}
	return nil
}
