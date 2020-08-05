package meta

import (
	"fmt"
	"server/errors"
	"strconv"
	"reflect"
	. "server/object/description"
)

type FieldDescription struct {
	*Field
	Meta           *Meta
	LinkMeta       *Meta
	OuterLinkField *FieldDescription
	LinkMetaList   *MetaList
	LinkThrough    *Meta
}

func (f *FieldDescription) IsValueTypeValid(v interface{}) bool {
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

func (field *FieldDescription) ValueFromString(v string) (interface{}, error) {
	switch field.Type {
	case FieldTypeString, FieldTypeDateTime, FieldTypeDate, FieldTypeTime:
		return v, nil
	case FieldTypeNumber:
		return strconv.ParseFloat(v, 64)
	case FieldTypeBool:
		return strconv.ParseBool(v)
	case FieldTypeObject:
		if field.LinkMeta.Key.Field.Type == FieldTypeString {
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
			fmt.Sprintf("Unsupported conversion from 'string' for the NewField type '%s'", field.Type),
			nil,
		)
	}
}

func (f *FieldDescription) ValueAsString(v interface{}) (string, error) {
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
			return f.LinkMeta.Key.ValueAsString(v)
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
func (f *FieldDescription) ReverseOuterField() *FieldDescription {
	if f.Type == FieldTypeObject && f.LinkType == LinkTypeInner {
		for _, field := range f.LinkMeta.Fields {
			if field.Type == FieldTypeArray && field.LinkType == LinkTypeOuter {
				if field.OuterLinkField.Name == f.Name && field.LinkMeta.Name == f.Meta.Name {
					return &field
				}
			}
		}
	}
	return nil
}
