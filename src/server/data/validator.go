package data

import (
	"server/object/meta"
	"server/data/errors"
	"server/data/validators"
	. "server/data/record"
	. "server/data/types"
	"server/object/description"
	"server/transactions"
)

type ValidationService struct {
	metaStore *meta.MetaStore
	processor *Processor
}

//TODO:this method needs deep refactoring
func (validationService *ValidationService) Validate(dbTransaction transactions.DbTransaction, record *Record, mandatoryCheck bool) ([]Record, error) {
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
		if valueIsSet {
			switch {
			case !(value == nil && fieldDescription.Optional) && fieldDescription.Type == description.FieldTypeString && description.FieldTypeNumber.AssertType(value):
				break
			case !(value == nil && fieldDescription.Optional) && fieldDescription.Type.AssertType(value):
				if fieldDescription.Type == description.FieldTypeArray {
					var a = value.([]interface{})
					for _, av := range a {
						if m, ok := av.(map[string]interface{}); ok {
							m[fieldDescription.OuterLinkField.Name] = ALink{Field: fieldDescription, IsOuter: true, Obj: record.Data}
							toCheck = append(toCheck, Record{fieldDescription.LinkMeta, m})
						} else if pkValue, ok := av.(interface{}); ok {
							toCheck = append(toCheck, Record{fieldDescription.LinkMeta, map[string]interface{}{
								fieldDescription.OuterLinkField.Name: record.Pk(),
								fieldDescription.LinkMeta.Key.Name:   pkValue,
							}})
						} else {
							return nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Array in field '%s' must contain only JSON object", k)
						}
					}
					delete(record.Data, k)
				} else if fieldDescription.Type == description.FieldTypeObject {
					var of = value.(map[string]interface{})
					if fieldDescription.LinkType == description.LinkTypeOuter {
						of[fieldDescription.OuterLinkField.Name] = ALink{Field: fieldDescription, IsOuter: true, Obj: record.Data}
						delete(record.Data, k)
					} else if fieldDescription.LinkType == description.LinkTypeInner {
						record.Data[fieldDescription.Name] = ALink{Field: fieldDescription.LinkMeta.Key, IsOuter: false, Obj: of}
					} else {
						return nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Unknown link type %s", fieldDescription.LinkType)
					}
					toCheck = append(toCheck, Record{fieldDescription.LinkMeta, of})
				} else if fieldDescription.IsSimple() && fieldDescription.LinkType == description.LinkTypeInner {
					record.Data[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, IsOuter: false, Id: value}
				}
			case !(value == nil && fieldDescription.Optional) && fieldDescription.Type == description.FieldTypeObject && fieldDescription.LinkType == description.LinkTypeInner && fieldDescription.LinkMeta.Key.Type.AssertType(value):
				record.Data[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, IsOuter: false, Id: value}
			case !(value == nil && fieldDescription.Optional) && fieldDescription.Type == description.FieldTypeObject && fieldDescription.LinkType == description.LinkTypeInner && AssertLink(value):
			case fieldDescription.Type == description.FieldTypeGeneric && fieldDescription.LinkType == description.LinkTypeInner:
				if value != nil {
					if record.Data[fieldDescription.Name], err = validators.NewGenericInnerFieldValidator(dbTransaction, validationService.metaStore.Get, validationService.processor.Get).Validate(fieldDescription, value); err != nil {
						return nil, err
					}
				} else {
					record.Data[fieldDescription.Name] = new(GenericInnerLink)
				}
			default:
				if !(value == nil && fieldDescription.Optional) {
					return nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Field '%s' has a wrong type", k)
				}
			}
		}
	}
	return toCheck, nil
}

func NewValidationService(metaStore *meta.MetaStore, processor *Processor) *ValidationService {
	return &ValidationService{metaStore: metaStore, processor: processor}
}
