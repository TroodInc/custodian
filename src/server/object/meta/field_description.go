package meta

import (
	"fmt"
	"reflect"
	"server/errors"
	"strconv"
)

type Field struct {
	Name           string       `json:"name"`
	Type           FieldType    `json:"type"`
	LinkType       LinkType     `json:"linkType,omitempty"`
	Optional       bool         `json:"optional"`
	Unique         bool         `json:"unique"`
	OnDelete       string       `json:"onDelete,omitempty"`
	Def            interface{}  `json:"default,omitempty"`
	NowOnUpdate    bool         `json:"nowOnUpdate,omitempty"`
	NowOnCreate    bool         `json:"nowOnCreate,omitempty"`
	QueryMode      bool         `json:"queryMode,omitempty"`    //only for outer links, true if field should be used for querying
	RetrieveMode   bool         `json:"retrieveMode,omitempty"` //only for outer links, true if field should be used for data retrieving

	Meta           *Meta

	LinkMeta       *Meta		`json:"linkMeta,omitempty"`     //only for array and "object"
	OuterLinkField *Field		`json:"outerLinkField,omitempty"`
	LinkMetaList   []*Meta		`json:"linkMetaList,omitempty"` //only for array and "object"
	LinkThrough    *Meta		`json:"linkThrough,omitempty"`  //only for "objects" field
}

func NewFieldFromMap(object map[string]interface{}) *Field {
	result := &Field{
		Name:           object["name"].(string),
		Type:           AsFieldType(object["type"].(string)),
		LinkType:       AsLinkType(object["linkType"].(string)),
		Optional:       object["optional"].(bool),
		Unique:         object["unique"].(bool),
		OnDelete:       object["onDelete"].(string),
		Def:            nil,
		NowOnUpdate:    false,
		NowOnCreate:    false,
	}

	return result
}

func (f *Field) ForExport() map[string]interface{} {
	result := map[string]interface{}{
		"name": f.Name,
		"type": f.Type.String(),
		"optional": f.Optional,
		"unique": f.Unique,
		"onDelete": f.OnDelete,
		"outerLinkField": f.Name,
		"linkMetaList": f.GetLinkMetaListNames(),
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
