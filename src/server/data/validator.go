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
func (validationService *ValidationService) Validate(t2 *Tuple2, mandatoryCheck bool) ([]Tuple2, error) {
	var err error
	toCheck := make([]Tuple2, 0)
	for k, _ := range t2.Second {
		if f := t2.First.FindField(k); f == nil {
			delete(t2.Second, k)
		}
	}

	for i := 0; i < len(t2.First.Fields); i++ {
		k := t2.First.Fields[i].Name
		fieldDescription := &t2.First.Fields[i]

		value, valueIsSet := t2.Second[k]
		if mandatoryCheck && !valueIsSet && !fieldDescription.Optional {
			return nil, errors.NewDataError(t2.First.Name, errors.ErrMandatoryFiledAbsent, "Not optional field '%s' is absent", k)
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
							m[fieldDescription.OuterLinkField.Name] = ALink{Field: fieldDescription, outer: true, Obj: t2.Second}
							toCheck = append(toCheck, Tuple2{fieldDescription.LinkMeta, m})
						} else {
							return nil, errors.NewDataError(t2.First.Name, errors.ErrWrongFiledType, "Array in field '%s' must contain only JSON object", k)
						}
					}
					delete(t2.Second, k)
				} else if fieldDescription.Type == meta.FieldTypeObject {
					var of = value.(map[string]interface{})
					if fieldDescription.LinkType == meta.LinkTypeOuter {
						of[fieldDescription.OuterLinkField.Name] = ALink{Field: fieldDescription, outer: true, Obj: t2.Second}
						delete(t2.Second, k)
					} else if fieldDescription.LinkType == meta.LinkTypeInner {
						t2.Second[fieldDescription.Name] = ALink{Field: fieldDescription.LinkMeta.Key, outer: false, Obj: of}
					} else {
						return nil, errors.NewDataError(t2.First.Name, errors.ErrWrongFiledType, "Unknown link type %s", fieldDescription.LinkType)
					}
					toCheck = append(toCheck, Tuple2{fieldDescription.LinkMeta, of})
				} else if fieldDescription.IsSimple() && fieldDescription.LinkType == meta.LinkTypeInner {
					t2.Second[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, outer: false, Id: value}
				}
			case fieldDescription.Type == meta.FieldTypeObject && fieldDescription.LinkType == meta.LinkTypeInner && fieldDescription.LinkMeta.Key.Type.AssertType(value):
				t2.Second[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, outer: false, Id: value}
			case fieldDescription.Type == meta.FieldTypeObject && fieldDescription.LinkType == meta.LinkTypeInner && AssertLink(value):
			case fieldDescription.Type == meta.FieldTypeGeneric && fieldDescription.LinkType == meta.LinkTypeInner:
				if t2.Second[fieldDescription.Name], err = validators.NewGenericInnerFieldValidator(validationService.metaStore.Get, validationService.processor.Get).Validate(fieldDescription, value); err != nil {
					return nil, err
				}
			default:
				return nil, errors.NewDataError(t2.First.Name, errors.ErrWrongFiledType, "Field '%s' has a wrong type", k)
			}

		}
	}
	return toCheck, nil
}

func NewValidationService(metaStore *meta.MetaStore, processor *Processor) *ValidationService {
	return &ValidationService{metaStore: metaStore, processor: processor}
}
