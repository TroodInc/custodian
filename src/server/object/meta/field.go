package meta

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"utils"
	"server/errors"
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
		return "", errors.NewFatalError("", "Unsupported column type: " + string(fieldType), nil)
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

type Field struct {
	Name         string      `json:"name"`
	Type         FieldType   `json:"type"`
	LinkType     LinkType    `json:"linkType,omitempty"`
	Optional     bool        `json:"optional"`
	Unique       bool        `json:"unique"`
	OnDelete     string      `json:"onDelete,omitempty"`
	Def          interface{} `json:"default,omitempty"`
	NowOnUpdate  bool        `json:"nowOnUpdate,omitempty"`
	NowOnCreate  bool        `json:"nowOnCreate,omitempty"`
	QueryMode    bool        `json:"queryMode,omitempty"`    //only for outer links, true if field should be used for querying
	RetrieveMode bool        `json:"retrieveMode,omitempty"` //only for outer links, true if field should be used for data retrieving

	Meta           *Meta

	LinkMeta       *Meta   `json:"linkMeta,omitempty"` //only for array and "object"
	OuterLinkField *Field  `json:"outerLinkField,omitempty"`
	LinkMetaList   []*Meta `json:"linkMetaList,omitempty"` //only for array and "object"
	LinkThrough    *Meta   `json:"linkThrough,omitempty"`  //only for "objects" field
}

func NewFieldFromMap(object map[string]interface{}) *Field {
	result := &Field{
		Name:        object["name"].(string),
		Type:        AsFieldType(object["type"].(string)),
		Optional:    object["optional"].(bool),
		Unique:      object["unique"].(bool),
		Def:         object["def"],
		NowOnUpdate: false,
		NowOnCreate: false,
		LinkMeta:    nil,
	}

	if object["onDelete"] != nil {
		result.OnDelete = object["onDelete"].(string)
	}


	if object["linkType"] != nil {
		result.LinkType = AsLinkType(object["linkType"].(string))
	}

	return result
}

func (f *Field) ForExport() map[string]interface{} {
	result := map[string]interface{}{
		"name": f.Name,
		"type": f.Type.String(),
		"optional": f.Optional,
		"unique": f.Unique,
		"def": f.Def,
		"onDelete": f.OnDelete,
		"linkMetaList": f.GetLinkMetaListNames(),
	}

	if 	f.OuterLinkField != nil {
		result["outerLinkField"] = f.OuterLinkField.Name
	}

	if f.LinkMeta != nil {
		result["linkMeta"] = f.LinkMeta.Name
	}

	return result
}

func (f *Field) GetLinkMetaListNames() []string {
	names := make([]string, 0)
	for _, meta := range f.LinkMetaList {
		names = append(names, meta.Name)
	}

	return names
}


func (f *Field) GetLinkMetaByName(name string) *Meta {
	for _, meta := range f.LinkMetaList {
		if meta.Name == name {
			return meta
		}
	}
	return nil
}

func (f *Field) IsSimple() bool {
	return f.Type != FieldTypeObject && f.Type != FieldTypeArray && f.Type != FieldTypeGeneric && f.Type != FieldTypeObjects
}

func (f *Field) IsLink() bool {
	return f.Type == FieldTypeObject || f.Type == FieldTypeArray || f.Type == FieldTypeGeneric || f.Type == FieldTypeObjects
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
		Unique:       	f.Unique,
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

func (f *Field) IsValueTypeValid(v interface{}) bool {
	switch f.Type {
	case FieldTypeString, FieldTypeDateTime, FieldTypeDate, FieldTypeTime:
		_, ok := v.(string)
		return ok
	case FieldTypeNumber:
		_, ok := v.(float64)
		return ok
	case FieldTypeBool:
		_, ok := v.(bool)
		return ok
	default:
		return false
	}

}

func (f *Field) ValueFromString(v string) (interface{}, error) {
	switch f.Type {
	case FieldTypeString, FieldTypeDateTime, FieldTypeDate, FieldTypeTime:
		return v, nil
	case FieldTypeNumber:
		return strconv.ParseFloat(v, 64)
	case FieldTypeBool:
		return strconv.ParseBool(v)
	case FieldTypeObject:
		if f.LinkMeta.GetKey().Type == FieldTypeString {
			return v, nil
		} else {
			if f, err := strconv.ParseFloat(v, 64); err != nil {
				return strconv.Atoi(v)
			} else {
				return f, nil
			}
		}
	case FieldTypeArray:
		return strconv.Atoi(v)
	case FieldTypeGeneric:
		//	case of querying by object
		return v, nil
	default:
		return nil, errors.NewValidationError(
			"pk_from_string_conversion",
			fmt.Sprintf("Unsupported conversion from 'string' for the NewField type '%s'", f.Type),
			nil,
		)
	}
}

func (f *Field) ValueAsString(v interface{}) (string, error) {
	switch f.Type {
	case FieldTypeString, FieldTypeDateTime, FieldTypeDate, FieldTypeTime:
		if str, ok := v.(string); !ok {
			return "", errors.NewValidationError(
				"conversion",
				fmt.Sprintf("Wrong input value type '%s'. For NewField '%s' expects 'string' type", reflect.TypeOf(v).String(), f.Name),
				nil,
			)
		} else {
			return str, nil
		}
	case FieldTypeNumber:
		switch value := v.(type) {
		case float64:
			return strconv.FormatFloat(value, 'f', -1, 64), nil
		case string:
			return value, nil
		default:
			return "", errors.NewValidationError(
				"conversion",
				fmt.Sprintf("Wrong input value type '%s'. For NewField '%s' expects 'float64' type", reflect.TypeOf(v).String(), f.Name),
				nil,
			)
		}
	case FieldTypeBool:
		if b, ok := v.(bool); !ok {
			return "", errors.NewValidationError(
				"conversion",
				fmt.Sprintf("Wrong input value type '%s'. For NewField '%s' expects 'bool' type", reflect.TypeOf(v).String(), f.Name),
				nil,
			)
		} else {
			return strconv.FormatBool(b), nil
		}
	case FieldTypeObject, FieldTypeArray:
		if f.LinkType == LinkTypeInner {
			linkKey := f.LinkMeta.FindField(f.LinkMeta.Key)
			return linkKey.ValueAsString(v)
		} else {
			return f.OuterLinkField.ValueAsString(v)
		}
	case FieldTypeGeneric:
		switch value := v.(type) {
		case float64:
			return strconv.FormatFloat(value, 'f', -1, 64), nil
		case string:
			return value, nil
		default:
			return "", errors.NewValidationError(
				"conversion",
				fmt.Sprintf("Wrong input value type '%s'. For NewField '%s' expects 'float64' type", reflect.TypeOf(v).String(), f.Name),
				nil,
			)
		}
	default:
		return "", errors.NewValidationError(
			"conversion",
			fmt.Sprintf("Unknown NewField type '%s'", f.Type),
			nil,
		)
	}
}

//TODO: actually is redundant, its usages should be replaced with MetaDescriptionManager.ReverseOuterField
func (f *Field) ReverseOuterField() *Field {
	if f.Type == FieldTypeObject && f.LinkType == LinkTypeInner {
		for _, field := range f.LinkMeta.Fields {
			if field.Type == FieldTypeArray && field.LinkType == LinkTypeOuter {
				if field.OuterLinkField.Name == f.Name && field.LinkMeta.Name == f.Meta.Name {
					return field
				}
			}
		}
	}
	return nil
}

func (f *Field) canBeLinkTo(m *Meta) bool {
	isSimpleFieldWithSameTypeAsPk := f.IsSimple() && f.Type == m.FindField(m.Key).Type
	isInnerLinkToMeta :=
		f.Type == FieldTypeObject &&
			f.LinkMeta.Name == m.Name &&
			f.LinkType == LinkTypeInner
	isGenericInnerLinkToMeta := f.Type == FieldTypeGeneric && f.LinkType == LinkTypeInner && utils.Contains(f.GetLinkMetaListNames(), m.Name)
	canBeLinkTo := isSimpleFieldWithSameTypeAsPk || isInnerLinkToMeta || isGenericInnerLinkToMeta
	return canBeLinkTo
}