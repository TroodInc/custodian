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
func (vs *ValidationService) Validate(dbTransaction transactions.DbTransaction, record *Record) ([]*RecordProcessingNode, []*RecordProcessingNode, []*RecordProcessingNode, []*RecordProcessingNode, error) {
	nodesToProcessBefore := make([]*RecordProcessingNode, 0)
	nodesToProcessAfter := make([]*RecordProcessingNode, 0)
	nodesToRemoveBefore := make([]*RecordProcessingNode, 0)
	nodesToRetrieveBefore := make([]*RecordProcessingNode, 0)
	for k, _ := range record.Data {
		if f := record.Meta.FindField(k); f == nil {
			delete(record.Data, k)
		}
	}

	for i := 0; i < len(record.Meta.Fields); i++ {
		fieldName := record.Meta.Fields[i].Name
		fieldDescription := &record.Meta.Fields[i]
		if fieldDescription.LinkType == description.LinkTypeOuter && !fieldDescription.RetrieveMode {
			continue
		}

		value, valueIsSet := record.Data[fieldName]
		if !valueIsSet && !fieldDescription.Optional && record.IsPhantom() {
			return nil, nil, nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrMandatoryFiledAbsent, "Not optional field '%s' is absent", fieldName)
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
					if childRecordsToProcess, childRecordsToRemove, err := vs.validateArray(dbTransaction, value, fieldDescription, record); err != nil {
						return nil, nil, nil, nil, err
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
					record.Data[fieldDescription.Name] = LazyLink{Field: fieldDescription.LinkMeta.Key, IsOuter: false, Obj: of}
					nodesToProcessBefore = append(nodesToProcessBefore, NewRecordProcessingNode(NewRecord(fieldDescription.LinkMeta, of)))
				} else if fieldDescription.Type == description.FieldTypeObjects {
					//validate outer links
					if childRecordsToProcess, childRecordsToRemove, childRecordsToRetrieve, err := vs.validateObjectsFieldArray(dbTransaction, value, fieldDescription, record); err != nil {
						return nil, nil, nil, nil, err
					} else {
						for _, childRecord := range childRecordsToProcess {
							nodesToProcessAfter = append(nodesToProcessAfter, NewRecordProcessingNode(childRecord))
						}
						for _, childRecord := range childRecordsToRetrieve {
							nodesToRetrieveBefore = append(nodesToRetrieveBefore, NewRecordProcessingNode(childRecord))
						}
						for _, childRecord := range childRecordsToRemove {
							nodesToRemoveBefore = append(nodesToRemoveBefore, NewRecordProcessingNode(childRecord))
						}
					}
					delete(record.Data, fieldName)
				} else if fieldDescription.IsSimple() && fieldDescription.LinkType == description.LinkTypeInner {
					record.Data[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, IsOuter: false, Id: value}
				}
			case fieldDescription.Type == description.FieldTypeObject && fieldDescription.LinkType == description.LinkTypeInner && (fieldDescription.LinkMeta.Key.Type.AssertType(value) || fieldDescription.Optional && value == nil ):
				record.Data[fieldDescription.Name] = DLink{Field: fieldDescription.LinkMeta.Key, IsOuter: false, Id: value}
			case fieldDescription.Type == description.FieldTypeGeneric && fieldDescription.LinkType == description.LinkTypeInner:
				if recordToProcess, err := vs.validateInnerGenericLink(dbTransaction, value, fieldDescription, record); err != nil {
					return nil, nil, nil, nil, err
				} else {
					if recordToProcess != nil {
						nodesToProcessBefore = append(nodesToProcessBefore, NewRecordProcessingNode(recordToProcess))
					}
				}
			case fieldDescription.Type == description.FieldTypeGeneric && fieldDescription.LinkType == description.LinkTypeOuter:
				//validate outer generic links
				if childRecordsToProcess, childRecordsToRemove, err := vs.validateGenericArray(dbTransaction, value, fieldDescription, record); err != nil {
					return nil, nil, nil, nil, err
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
				if _, ok := value.(LazyLink); ok {
					break
				} else if value != nil {
					return nil, nil, nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Field '%s' has a wrong type", fieldName)
				}
			}
		}
	}
	return nodesToRetrieveBefore, nodesToProcessBefore, nodesToProcessAfter, nodesToRemoveBefore, nil
}

func (vs *ValidationService) validateArray(dbTransaction transactions.DbTransaction, value interface{}, fieldDescription *meta.FieldDescription, record *Record) ([]*Record, []*Record, error) {
	var nestedRecordsData = value.([]interface{})
	recordsToProcess := make([]*Record, len(nestedRecordsData))
	recordsToRemove := make([]*Record, 0)
	for i, recordData := range nestedRecordsData {
		if recordDataAsMap, ok := recordData.(map[string]interface{}); ok {
			//new record`s data case
			recordDataAsMap[fieldDescription.OuterLinkField.Name] = LazyLink{Field: fieldDescription, IsOuter: true, Obj: record.Data, Index: i, NeighboursCount: len(nestedRecordsData)}
			recordsToProcess[i] = NewRecord(fieldDescription.LinkMeta, recordDataAsMap)
		} else if pkValue, ok := recordData.(interface{}); ok {
			recordsToProcess[i] = NewRecord(fieldDescription.LinkMeta, map[string]interface{}{
				fieldDescription.OuterLinkField.Name: LazyLink{Field: fieldDescription, IsOuter: true, Obj: record.Data, Index: i, NeighboursCount: len(nestedRecordsData)},
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
			vs.processor.GetBulk(dbTransaction, fieldDescription.LinkMeta.Name, filter, 1, true, callbackFunction)
		}
		if len(recordsToRemove) > 0 {
			if fieldDescription.OuterLinkField.OnDelete == description.OnDeleteRestrict {
				return nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrRestrictConstraintViolation, "Array in field '%s'contains records, which could not be removed due to `Restrict` strategy set", fieldDescription.Name)
			}
		}
	}
	return recordsToProcess, recordsToRemove, nil
}

func (vs *ValidationService) validateObjectsFieldArray(dbTransaction transactions.DbTransaction, value interface{}, fieldDescription *meta.FieldDescription, record *Record) ([]*Record, []*Record, []*Record, error) {
	var nestedRecordsData = value.([]interface{})
	recordsToProcess := make([]*Record, 0)
	recordsToRemove := make([]*Record, 0)
	recordsToRetrieve := make([]*Record, 0)
	for i, recordData := range nestedRecordsData {
		//new record`s data case
		if recordDataAsMap, ok := recordData.(map[string]interface{}); ok {
			//create a record of specified object with LazyLink to a parent record
			linkedRecord := NewRecord(fieldDescription.LinkMeta, recordDataAsMap)
			linkedRecord.Links = append(linkedRecord.Links, &LazyLink{
				Field:           fieldDescription,
				IsOuter:         true,
				Obj:             record.Data,
				Index:           i,
				NeighboursCount: len(nestedRecordsData),
			})
			recordsToProcess = append(recordsToProcess, linkedRecord)
			//create a record of through object
			linkThroughRecord := NewRecord(fieldDescription.LinkThrough,
				map[string]interface{}{
					fieldDescription.Meta.Name: LazyLink{
						Field:           fieldDescription.LinkThrough.FindField(fieldDescription.Meta.Name),
						IsOuter:         true,
						Obj:             record.Data,
						Index:           i,
						NeighboursCount: len(nestedRecordsData),
					},
					fieldDescription.LinkMeta.Name: LazyLink{
						Field:           fieldDescription.LinkThrough.FindField(fieldDescription.LinkMeta.Name),
						IsOuter:         true,
						Obj:             linkedRecord.Data,
						Index:           i,
						NeighboursCount: len(nestedRecordsData),
					},
				},
			)
			recordsToProcess = append(recordsToProcess, linkThroughRecord)
		} else if pkValue, ok := recordData.(interface{}); ok {
			//existing record`s ID case

			//create a record of through object containing ID of existing record and Link to a record being created
			linkThroughRecord := NewRecord(
				fieldDescription.LinkThrough,
				map[string]interface{}{
					fieldDescription.Meta.Name: LazyLink{
						Field:           fieldDescription.LinkThrough.FindField(fieldDescription.Meta.Name),
						IsOuter:         true,
						Obj:             record.Data,
						Index:           i,
						NeighboursCount: len(nestedRecordsData),
					},
					fieldDescription.LinkMeta.Name: pkValue,
				},
			)
			recordsToProcess = append(recordsToProcess, linkThroughRecord)

			// add a referenced record into retrieve queue
			referencedRecord := NewRecord(
				fieldDescription.LinkMeta,
				map[string]interface{}{
					fieldDescription.LinkMeta.Key.Name: pkValue,
				},
			)
			referencedRecord.Links = append(referencedRecord.Links, &LazyLink{
				Field:           fieldDescription,
				IsOuter:         true,
				Obj:             record.Data,
				Index:           i,
				NeighboursCount: len(nestedRecordsData),
			})
			recordsToRetrieve = append(recordsToRetrieve, referencedRecord)

		} else {
			return nil, nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Array in field '%s' must contain only JSON objects", fieldDescription.Name)
		}
	}

	beingAddedIds := ""

	for _, record := range recordsToProcess {
		if pkValue, ok := record.Data[fieldDescription.LinkMeta.Name].(interface{}); ok {
			idAsString, _ := fieldDescription.LinkMeta.Key.ValueAsString(pkValue)
			if len(beingAddedIds) != 0 {
				beingAddedIds += ","
			}
			beingAddedIds += idAsString
		}
	}

	//get records which are not presented in data and should be removed from m2m relation
	if !record.IsPhantom() {
		if len(beingAddedIds) > 0 {
			filter := fmt.Sprintf("eq(%s,%s),not(in(%s,(%s)))", fieldDescription.Meta.Name, record.PkAsString(), fieldDescription.LinkMeta.Name, beingAddedIds)
			callbackFunction := func(obj map[string]interface{}) error {
				recordsToRemove = append(recordsToRemove, NewRecord(fieldDescription.LinkThrough, obj))
				return nil
			}
			vs.processor.GetBulk(dbTransaction, fieldDescription.LinkThrough.Name, filter, 1, true, callbackFunction)
		}
	}
	//get records which are already attached and remove them from list of records to process
	if !record.IsPhantom() {
		if len(beingAddedIds) > 0 {
			filter := fmt.Sprintf("eq(%s,%s),in(%s,(%s))", fieldDescription.Meta.Name, record.PkAsString(), fieldDescription.LinkMeta.Name, beingAddedIds)
			callbackFunction := func(obj map[string]interface{}) error {
				removedCount := 0
				for i := range recordsToProcess {
					j := i - removedCount
					if recordsToProcess[j].Data[fieldDescription.LinkMeta.Name] == obj[fieldDescription.LinkMeta.Name] {
						recordsToProcess = append(recordsToProcess[:j], recordsToProcess[j+1:]...)
						removedCount++
					}
				}
				return nil
			}
			vs.processor.GetBulk(dbTransaction, fieldDescription.LinkThrough.Name, filter, 1, true, callbackFunction)
		}
	}
	return recordsToProcess, recordsToRemove, recordsToRetrieve, nil
}

func (vs *ValidationService) validateGenericArray(dbTransaction transactions.DbTransaction, value interface{}, fieldDescription *meta.FieldDescription, record *Record) ([]*Record, []*Record, error) {
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

func (vs *ValidationService) validateInnerGenericLink(dbTransaction transactions.DbTransaction, value interface{}, fieldDescription *meta.FieldDescription, record *Record) (*Record, error) {
	var err error
	var recordToProcess *Record
	if value != nil {
		//if value is already set by "validateGenericArray"
		if _, ok := value.(*AGenericInnerLink); ok {
			return nil, nil
		}
		if record.Data[fieldDescription.Name], err = validators.NewGenericInnerFieldValidator(dbTransaction, vs.metaStore.Get, vs.processor.Get).Validate(fieldDescription, value); err != nil {
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
