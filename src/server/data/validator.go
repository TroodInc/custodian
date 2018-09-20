package data

import (
	"server/object/meta"
	"server/data/errors"
	"server/data/validators"
	. "server/data/record"
	. "server/data/types"
	"server/object/description"
	"server/transactions"
	"fmt"
)

type ValidationService struct {
	metaStore *meta.MetaStore
	processor *Processor
}

//TODO:this method needs deep refactoring
func (validationService *ValidationService) Validate(dbTransaction transactions.DbTransaction, record *Record) ([]*RecordProcessingNode, []*RecordProcessingNode, []*RecordProcessingNode, error) {
	nodesToProcessBefore := make([]*RecordProcessingNode, 0)
	nodesToProcessAfter := make([]*RecordProcessingNode, 0)
	nodesToRemoveBefore := make([]*RecordProcessingNode, 0)
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
			return nil, nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrMandatoryFiledAbsent, "Not optional field '%s' is absent", fieldName)
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
					if childRecordsToProcess, childRecordsToRemove, err := validationService.validateArray(dbTransaction, value, fieldDescription, record); err != nil {
						return nil, nil, nil, err
					} else {
						for _, childRecord := range childRecordsToProcess {
							nodesToProcessAfter = append(nodesToProcessAfter, NewRecordProcessingNode(childRecord))
						}
						for _, childRecord := range childRecordsToRemove {
							nodesToRemoveBefore = append(nodesToRemoveBefore, NewRecordProcessingNode(childRecord))
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
						return nil, nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Unknown link type %s", fieldDescription.LinkType)
					}
					nodesToProcessBefore = append(nodesToProcessBefore, NewRecordProcessingNode(NewRecord(fieldDescription.LinkMeta, of)))
				} else if fieldDescription.IsSimple() && fieldDescription.LinkType == description.LinkTypeInner {
					record.Data[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, IsOuter: false, Id: value}
				}
			case !(value == nil && fieldDescription.Optional) && fieldDescription.Type == description.FieldTypeObject && fieldDescription.LinkType == description.LinkTypeInner && fieldDescription.LinkMeta.Key.Type.AssertType(value):
				record.Data[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, IsOuter: false, Id: value}
			case fieldDescription.Type == description.FieldTypeGeneric && fieldDescription.LinkType == description.LinkTypeInner:
				if recordToProcess, err := validationService.validateInnerGenericLink(dbTransaction, value, fieldDescription, record); err != nil {
					return nil, nil, nil, err
				} else {
					if recordToProcess != nil {
						nodesToProcessBefore = append(nodesToProcessBefore, NewRecordProcessingNode(recordToProcess))
					}
				}
			case fieldDescription.Type == description.FieldTypeGeneric && fieldDescription.LinkType == description.LinkTypeOuter:
				//validate outer generic links
				if childRecordsToProcess, childRecordsToRemove, err := validationService.validateGenericArray(dbTransaction, value, fieldDescription, record); err != nil {
					return nil, nil, nil, err
				} else {
					for _, childRecord := range childRecordsToProcess {
						nodesToProcessAfter = append(nodesToProcessAfter, NewRecordProcessingNode(childRecord))
					}
					for _, childRecord := range childRecordsToRemove {
						nodesToRemoveBefore = append(nodesToRemoveBefore, NewRecordProcessingNode(childRecord))
					}
				}
				delete(record.Data, fieldName)
			default:
				if !(value == nil && fieldDescription.Optional) {
					return nil, nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Field '%s' has a wrong type", fieldName)
				}
			}
		}
	}
	return nodesToProcessBefore, nodesToProcessAfter, nodesToRemoveBefore, nil
}

func (validationService *ValidationService) validateArray(dbTransaction transactions.DbTransaction, value interface{}, fieldDescription *meta.FieldDescription, record *Record) ([]*Record, []*Record, error) {
	var nestedRecordsData = value.([]interface{})
	recordsToProcess := make([]*Record, len(nestedRecordsData))
	recordsToRemove := make([]*Record, 0)
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
			return nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Array in field '%s' must contain only JSON object", fieldDescription.Name)
		}
	}

	//get records which are not presented in data
	if !record.IsPhantom() {
		idFilters := ""
		for _, record := range recordsToProcess {
			if !record.IsPhantom() {
				idAsString, _ := record.Meta.Key.ValueAsString(record.Pk())
				if len(idFilters) != 0 {
					idFilters += ","
				}
				idFilters += idAsString
			}
		}
		if len(idFilters) > 0 {
			filter := fmt.Sprintf("eq(%s,%s),not(eq(%s,%s))", fieldDescription.OuterLinkField.Name, record.PkAsString(), fieldDescription.LinkMeta.Key.Name, idFilters)
			callbackFunction := func(obj map[string]interface{}) error {
				if fieldDescription.OuterLinkField.OnDelete == description.OnDeleteCascade || fieldDescription.OuterLinkField.OnDelete == description.OnDeleteRestrict {
					recordsToRemove = append(recordsToRemove, NewRecord(fieldDescription.LinkMeta, obj))
				} else if fieldDescription.OuterLinkField.OnDelete == description.OnDeleteSetNull {
					obj[fieldDescription.OuterLinkField.Name] = nil
					recordsToProcess = append(recordsToProcess, NewRecord(fieldDescription.LinkMeta, obj))
				}
				return nil
			}
			validationService.processor.GetBulk(dbTransaction, fieldDescription.LinkMeta.Name, filter, 1, callbackFunction)
		}
		if len(recordsToRemove) > 0 {
			if fieldDescription.OuterLinkField.OnDelete == description.OnDeleteRestrict {
				return nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrRestrictConstraintViolation, "Array in field '%s'contains records, which could not be removed due to `Restrict` strategy set", fieldDescription.Name)
			}
		}
	}
	return recordsToProcess, recordsToRemove, nil
}

func (validationService *ValidationService) validateGenericArray(dbTransaction transactions.DbTransaction, value interface{}, fieldDescription *meta.FieldDescription, record *Record) ([]*Record, []*Record, error) {
	var nestedRecordsData = value.([]interface{})
	recordsToProcess := make([]*Record, len(nestedRecordsData))
	recordsToRemove := make([]*Record, 0)
	for i, recordData := range nestedRecordsData {
		if recordDataAsMap, ok := recordData.(map[string]interface{}); ok {
			recordDataAsMap[fieldDescription.OuterLinkField.Name] = &AGenericInnerLink{
				Field:           fieldDescription,
				LinkType:        description.LinkTypeOuter,
				RecordData:      record.Data,
				Index:           i,
				NeighboursCount: len(nestedRecordsData),
				GenericInnerLink: &GenericInnerLink{
					ObjectName:       fieldDescription.Meta.Name,
					Pk:               nil,
					FieldDescription: fieldDescription,
					PkName:           fieldDescription.Meta.Key.Name,
				},
			}
			recordsToProcess[i] = NewRecord(fieldDescription.LinkMeta, recordDataAsMap)
		} else if pkValue, ok := recordData.(interface{}); ok {
			recordsToProcess[i] = NewRecord(fieldDescription.LinkMeta, map[string]interface{}{fieldDescription.OuterLinkField.Name: &AGenericInnerLink{
				Field:           fieldDescription,
				LinkType:        description.LinkTypeOuter,
				RecordData:      record.Data,
				Index:           i,
				NeighboursCount: len(nestedRecordsData),
				GenericInnerLink: &GenericInnerLink{
					ObjectName:       fieldDescription.Meta.Name,
					Pk:               nil,
					FieldDescription: fieldDescription,
					PkName:           fieldDescription.Meta.Key.Name,
				},
			}, fieldDescription.LinkMeta.Key.Name: pkValue})
		} else {
			return nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Value in field '%s' has invalid value", fieldDescription.Name)
		}
	}

	return recordsToProcess, recordsToRemove, nil
}

func (validationService *ValidationService) validateInnerGenericLink(dbTransaction transactions.DbTransaction, value interface{}, fieldDescription *meta.FieldDescription, record *Record) (*Record, error) {
	var err error
	var recordToProcess *Record
	if value != nil {
		//if value is already set by "validateGenericArray"
		if _, ok := value.(*AGenericInnerLink); ok {
			return nil, nil
		}
		if record.Data[fieldDescription.Name], err = validators.NewGenericInnerFieldValidator(dbTransaction, validationService.metaStore.Get, validationService.processor.Get).Validate(fieldDescription, value); err != nil {
			if _, ok := err.(errors.GenericFieldPkIsNullError); ok {
				recordValuesAsMap := value.(map[string]interface{})
				objMeta := fieldDescription.LinkMetaList.GetByName(recordValuesAsMap[GenericInnerLinkObjectKey].(string))
				delete(recordValuesAsMap, GenericInnerLinkObjectKey)
				record.Data[fieldDescription.Name] = &AGenericInnerLink{
					GenericInnerLink: &GenericInnerLink{objMeta.Name, nil, fieldDescription, objMeta.Key.Name},
					Field:            fieldDescription,
					RecordData:       recordValuesAsMap,
					Index:            0,
					NeighboursCount:  1,
					LinkType:         description.LinkTypeInner,
				}
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
