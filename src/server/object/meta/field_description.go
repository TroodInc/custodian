package meta

import (
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
		return nil, NewMetaError(field.Meta.Name, "pk_from_string_conversion", ErrInternal, "Unsupported conversion from 'string' for the NewField type '%s'", field.Type)
	}
}

func (f *FieldDescription) ValueAsString(v interface{}) (string, error) {
	switch f.Type {
	case FieldTypeString, FieldTypeDateTime, FieldTypeDate, FieldTypeTime:
		if str, ok := v.(string); !ok {
			return "", NewMetaError(f.Meta.Name, "conversion", ErrInternal,
				"Wrong input value type '%s'. For NewField '%s' expects 'string' type", reflect.TypeOf(v).String(), f.Name)
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
			return "", NewMetaError(f.Meta.Name, "conversion", ErrInternal,
				"Wrong input value type '%s'. For NewField '%s' expects 'float64' type", reflect.TypeOf(v).String(), f.Name)
		}
	case FieldTypeBool:
		if b, ok := v.(bool); !ok {
			return "", NewMetaError(f.Meta.Name, "conversion", ErrInternal,
				"Wrong input value type '%s'. For NewField '%s' expects 'bool' type", reflect.TypeOf(v).String(), f.Name)
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
			return "", NewMetaError(f.Meta.Name, "conversion", ErrInternal,
				"Wrong input value type '%s'. For NewField '%s' expects 'float64' type", reflect.TypeOf(v).String(), f.Name)
		}
	default:
		return "", NewMetaError(f.Meta.Name, "conversion", ErrInternal, "Unknown NewField type '%s'", f.Type)
	}
}

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
