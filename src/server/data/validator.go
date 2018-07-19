package data

import (
	"server/meta"
	"server/data/errors"
	"server/data/validators"
)

type ValidationService struct {
	metaStore *meta.MetaStore
	processor *Processor
}

//TODO:this method needs deep refactoring
func (validationService *ValidationService) Validate(record *Record, mandatoryCheck bool) ([]Record, error) {
	var err error
	toCheck := make([]Record, 0)
	for k, _ := range record.Data {
		if f := record.Meta.FindField(k); f == nil {
			delete(record.Data, k)
		}
	}

	for i := 0; i < len(record.Meta.Fields); i++ {
		k := record.Meta.Fields[i].Name
		fieldDescription := &record.Meta.Fields[i]

		value, valueIsSet := record.Data[k]
		if mandatoryCheck && !valueIsSet && !fieldDescription.Optional {
			return nil, errors.NewDataError(record.Meta.Name, errors.ErrMandatoryFiledAbsent, "Not optional field '%s' is absent", k)
		}
		//skip validation if field is optional and value is null
		//perform validation otherwise
		if valueIsSet && !(value == nil && fieldDescription.Optional) {
			switch {
			case fieldDescription.Type == meta.FieldTypeString && meta.FieldTypeNumber.AssertType(value):
				break
			case fieldDescription.Type.AssertType(value):
				if fieldDescription.Type == meta.FieldTypeArray {
					var a = value.([]interface{})
					for _, av := range a {
						if m, ok := av.(map[string]interface{}); ok {
							m[fieldDescription.OuterLinkField.Name] = ALink{Field: fieldDescription, outer: true, Obj: record.Data}
							toCheck = append(toCheck, Record{fieldDescription.LinkMeta, m})
						} else {
							return nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Array in field '%s' must contain only JSON object", k)
						}
					}
					delete(record.Data, k)
				} else if fieldDescription.Type == meta.FieldTypeObject {
					var of = value.(map[string]interface{})
					if fieldDescription.LinkType == meta.LinkTypeOuter {
						of[fieldDescription.OuterLinkField.Name] = ALink{Field: fieldDescription, outer: true, Obj: record.Data}
						delete(record.Data, k)
					} else if fieldDescription.LinkType == meta.LinkTypeInner {
						record.Data[fieldDescription.Name] = ALink{Field: fieldDescription.LinkMeta.Key, outer: false, Obj: of}
					} else {
						return nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Unknown link type %s", fieldDescription.LinkType)
					}
					toCheck = append(toCheck, Record{fieldDescription.LinkMeta, of})
				} else if fieldDescription.IsSimple() && fieldDescription.LinkType == meta.LinkTypeInner {
					record.Data[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, outer: false, Id: value}
				}
			case fieldDescription.Type == meta.FieldTypeObject && fieldDescription.LinkType == meta.LinkTypeInner && fieldDescription.LinkMeta.Key.Type.AssertType(value):
				record.Data[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, outer: false, Id: value}
			case fieldDescription.Type == meta.FieldTypeObject && fieldDescription.LinkType == meta.LinkTypeInner && AssertLink(value):
			case fieldDescription.Type == meta.FieldTypeGeneric && fieldDescription.LinkType == meta.LinkTypeInner:
				if record.Data[fieldDescription.Name], err = validators.NewGenericInnerFieldValidator(validationService.metaStore.Get, validationService.processor.Get).Validate(fieldDescription, value); err != nil {
					return nil, err
				}
			default:
				return nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Field '%s' has a wrong type", k)
			}

		}
	}
	return toCheck, nil
}

func NewValidationService(metaStore *meta.MetaStore, processor *Processor) *ValidationService {
	return &ValidationService{metaStore: metaStore, processor: processor}
}
