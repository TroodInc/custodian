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
func (validationService *ValidationService) Validate(dbTransaction transactions.DbTransaction, record *Record) ([]*RecordProcessingNode, []*RecordProcessingNode, error) {
	nodesToProcessBefore := make([]*RecordProcessingNode, 0)
	nodesToProcessAfter := make([]*RecordProcessingNode, 0)
	for k, _ := range record.Data {
		if f := record.Meta.FindField(k); f == nil {
			delete(record.Data, k)
		}
	}

	for i := 0; i < len(record.Meta.Fields); i++ {
		fieldName := record.Meta.Fields[i].Name
		fieldDescription := &record.Meta.Fields[i]

		value, valueIsSet := record.Data[fieldName]
		if !valueIsSet && !fieldDescription.Optional && record.IsPhantom() {
			return nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrMandatoryFiledAbsent, "Not optional field '%s' is absent", fieldName)
		}
		//skip validation if field is optional and value is null
		//perform validation otherwise
		if valueIsSet {
			switch {
			case !(value == nil && fieldDescription.Optional) && fieldDescription.Type == description.FieldTypeString && description.FieldTypeNumber.AssertType(value):
				break
			case value != nil && fieldDescription.Type.AssertType(value):
				if fieldDescription.Type == description.FieldTypeArray {
					//validate outer links
					if childRecordsToProcess, err := validationService.validateArray(value, fieldDescription, record); err != nil {
						return nil, nil, err
					} else {
						for _, childRecord := range childRecordsToProcess {
							nodesToProcessAfter = append(nodesToProcessAfter, NewRecordProcessingNode(childRecord))
						}

					}
					delete(record.Data, fieldName)
				} else if fieldDescription.Type == description.FieldTypeObject {
					//TODO: move to separate method
					var of = value.(map[string]interface{})
					if fieldDescription.LinkType == description.LinkTypeOuter {
						of[fieldDescription.OuterLinkField.Name] = ALink{Field: fieldDescription, IsOuter: true, Obj: record.Data}
						delete(record.Data, fieldName)
					} else if fieldDescription.LinkType == description.LinkTypeInner {
						record.Data[fieldDescription.Name] = ALink{Field: fieldDescription.LinkMeta.Key, IsOuter: false, Obj: of}
					} else {
						return nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Unknown link type %s", fieldDescription.LinkType)
					}
					nodesToProcessBefore = append(nodesToProcessBefore, NewRecordProcessingNode(NewRecord(fieldDescription.LinkMeta, of)))
				} else if fieldDescription.IsSimple() && fieldDescription.LinkType == description.LinkTypeInner {
					record.Data[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, IsOuter: false, Id: value}
				}
			case !(value == nil && fieldDescription.Optional) && fieldDescription.Type == description.FieldTypeObject && fieldDescription.LinkType == description.LinkTypeInner && fieldDescription.LinkMeta.Key.Type.AssertType(value):
				record.Data[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, IsOuter: false, Id: value}
			case !(value == nil && fieldDescription.Optional) && fieldDescription.Type == description.FieldTypeObject && fieldDescription.LinkType == description.LinkTypeInner && AssertLink(value):
			case fieldDescription.Type == description.FieldTypeGeneric && fieldDescription.LinkType == description.LinkTypeInner:
				if recordToProcess, err := validationService.validateInnerGenericLink(dbTransaction, value, fieldDescription, record); err != nil {
					return nil, nil, err
				} else {
					if recordToProcess != nil {
						nodesToProcessBefore = append(nodesToProcessBefore, NewRecordProcessingNode(recordToProcess))
					}
				}

			case fieldDescription.Type == description.FieldTypeGeneric && fieldDescription.LinkType == description.LinkTypeOuter:
				//TODO:IMPLEMENT
				continue
			default:
				if !(value == nil && fieldDescription.Optional) {
					return nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Field '%s' has a wrong type", fieldName)
				}
			}
		}
	}
	return nodesToProcessBefore, nodesToProcessAfter, nil
}

func (validationService *ValidationService) validateArray(value interface{}, fieldDescription *meta.FieldDescription, record *Record) ([]*Record, error) {
	var nestedRecordsData = value.([]interface{})
	recordsToProcess := make([]*Record, len(nestedRecordsData))
	for i, recordData := range nestedRecordsData {
		if recordDataAsMap, ok := recordData.(map[string]interface{}); ok {
			//new record`s data case
			recordDataAsMap[fieldDescription.OuterLinkField.Name] = ALink{Field: fieldDescription, IsOuter: true, Obj: record.Data, Index: i, NeighboursCount: len(nestedRecordsData)}
			recordsToProcess[i] = NewRecord(fieldDescription.LinkMeta, recordDataAsMap)
		} else if pkValue, ok := recordData.(interface{}); ok {
			recordsToProcess[i] = NewRecord(fieldDescription.LinkMeta, map[string]interface{}{
				fieldDescription.OuterLinkField.Name: ALink{Field: fieldDescription, IsOuter: true, Obj: record.Data, Index: i, NeighboursCount: len(nestedRecordsData)},
				fieldDescription.LinkMeta.Key.Name:   pkValue,
			})
		} else {
			return nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Array in field '%s' must contain only JSON object", fieldDescription.Name)
		}
	}
	return recordsToProcess, nil
}

func (validationService *ValidationService) validateInnerGenericLink(dbTransaction transactions.DbTransaction, value interface{}, fieldDescription *meta.FieldDescription, record *Record) (*Record, error) {
	var err error
	var recordToProcess *Record
	if value != nil {
		if record.Data[fieldDescription.Name], err = validators.NewGenericInnerFieldValidator(dbTransaction, validationService.metaStore.Get, validationService.processor.Get).Validate(fieldDescription, value); err != nil {
			if _, ok := err.(errors.GenericFieldPkIsNullError); ok {
				recordValuesAsMap := value.(map[string]interface{})
				objMeta := fieldDescription.LinkMetaList.GetByName(recordValuesAsMap[GenericInnerLinkObjectKey].(string))
				delete(recordValuesAsMap, GenericInnerLinkObjectKey)
				record.Data[fieldDescription.Name] = &GenericInnerLink{objMeta.Name, nil, fieldDescription, objMeta.Key.Name, recordValuesAsMap}
				recordToProcess = NewRecord(objMeta, recordValuesAsMap)
			} else {
				return nil, err
			}
		}
	} else {
		record.Data[fieldDescription.Name] = new(GenericInnerLink)
	}
	return recordToProcess, nil
}

func NewValidationService(metaStore *meta.MetaStore, processor *Processor) *ValidationService {
	return &ValidationService{metaStore: metaStore, processor: processor}
}
