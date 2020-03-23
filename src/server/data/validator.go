package data

import (
	"fmt"
	"server/data/errors"
	. "server/data/record"
	. "server/data/types"
	"server/data/validators"
	"server/object/meta"
	"server/transactions"
)

type ValidationService struct {
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

	for _, field := range record.Meta.Fields {
		if field.LinkType == meta.LinkTypeOuter && !field.RetrieveMode {
			continue
		}

		value, valueIsSet := record.Data[field.Name]
		if !valueIsSet && !field.Optional && record.IsPhantom() {
			return nil, nil, nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrMandatoryFiledAbsent, "Not optional field '%s' is absent", field.Name)
		}
		//skip validation if field is optional and value is null
		//perform validation otherwise
		if valueIsSet {
			switch {
			case !(value == nil && field.Optional) && field.Type == meta.FieldTypeString && meta.FieldTypeNumber.AssertType(value):
				break
			case value != nil && field.Type.AssertType(value):
				if field.Type == meta.FieldTypeArray {
					//validate outer links
					if childRecordsToProcess, childRecordsToRemove, err := vs.validateArray(dbTransaction, value, field, record); err != nil {
						return nil, nil, nil, nil, err
					} else {
						for _, childRecord := range childRecordsToProcess {
							nodesToProcessAfter = append(nodesToProcessAfter, NewRecordProcessingNode(childRecord))
						}
						for _, childRecord := range childRecordsToRemove {
							nodesToRemoveBefore = append(nodesToRemoveBefore, NewRecordProcessingNode(childRecord))
						}

					}
					delete(record.Data, field.Name)
				} else if field.Type == meta.FieldTypeObject {
					//TODO: move to separate method
					var of = value.(map[string]interface{})
					record.Data[field.Name] = LazyLink{Field: field.LinkMeta.GetKey(), IsOuter: false, Obj: of}
					nodesToProcessBefore = append(nodesToProcessBefore, NewRecordProcessingNode(NewRecord(field.LinkMeta, of)))
				} else if field.Type == meta.FieldTypeObjects {
					//validate outer links
					if childRecordsToProcess, childRecordsToRemove, childRecordsToRetrieve, err := vs.validateObjectsFieldArray(value, field, record); err != nil {
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
					delete(record.Data, field.Name)
				} else if field.IsSimple() && field.LinkType == meta.LinkTypeInner {
					record.Data[field.Name] = DLink{Field: field.LinkMeta.GetKey(), IsOuter: false, Id: value}
				}
			case field.Type == meta.FieldTypeObject && field.LinkType == meta.LinkTypeInner && (field.LinkMeta.GetKey().Type.AssertType(value) || field.Optional && value == nil ):
				record.Data[field.Name] = DLink{Field: field.LinkMeta.GetKey(), IsOuter: false, Id: value}
			case field.Type == meta.FieldTypeGeneric && field.LinkType == meta.LinkTypeInner:
				if recordToProcess, err := vs.validateInnerGenericLink(dbTransaction, value, field, record); err != nil {
					return nil, nil, nil, nil, err
				} else {
					if recordToProcess != nil {
						nodesToProcessBefore = append(nodesToProcessBefore, NewRecordProcessingNode(recordToProcess))
					}
				}
			case field.Type == meta.FieldTypeGeneric && field.LinkType == meta.LinkTypeOuter:
				//validate outer generic links
				if childRecordsToProcess, childRecordsToRemove, err := vs.validateGenericArray(value, field, record); err != nil {
					return nil, nil, nil, nil, err
				} else {
					for _, childRecord := range childRecordsToProcess {
						nodesToProcessAfter = append(nodesToProcessAfter, NewRecordProcessingNode(childRecord))
					}
					for _, childRecord := range childRecordsToRemove {
						nodesToRemoveBefore = append(nodesToRemoveBefore, NewRecordProcessingNode(childRecord))
					}
				}
				delete(record.Data, field.Name)
			default:
				if _, ok := value.(LazyLink); ok {
					break
				} else if value != nil {
					return nil, nil, nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Field '%s' has a wrong type", field.Name)
				}
			}
		}
	}
	return nodesToRetrieveBefore, nodesToProcessBefore, nodesToProcessAfter, nodesToRemoveBefore, nil
}

func (vs *ValidationService) validateArray(dbTransaction transactions.DbTransaction, value interface{}, fieldDescription *meta.Field, record *Record) ([]*Record, []*Record, error) {
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
				fieldDescription.LinkMeta.Key:   pkValue,
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
				idAsString, _ := record.Meta.GetKey().ValueAsString(record.Pk())
				if len(idFilters) != 0 {
					idFilters += ","
				}
				idFilters += idAsString
			}
		}
		if len(idFilters) > 0 {
			filter := fmt.Sprintf("eq(%s,%s),not(eq(%s,%s))", fieldDescription.OuterLinkField.Name, record.PkAsString(), fieldDescription.LinkMeta.Key, idFilters)
			_, records, _ := vs.processor.GetBulk(fieldDescription.LinkMeta.Name, filter, nil, nil, 1, true)
			if *fieldDescription.OuterLinkField.OnDeleteStrategy() == meta.OnDeleteCascade || *fieldDescription.OuterLinkField.OnDeleteStrategy() == meta.OnDeleteRestrict {
				recordsToRemove = records
			} else if *fieldDescription.OuterLinkField.OnDeleteStrategy() == meta.OnDeleteSetNull {
				for _, item := range records {
					item.Data[fieldDescription.OuterLinkField.Name] = nil
					recordsToProcess = append(recordsToProcess, item)
				}
			}
		}
		if len(recordsToRemove) > 0 {
			if *fieldDescription.OuterLinkField.OnDeleteStrategy() == meta.OnDeleteRestrict {
				return nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrRestrictConstraintViolation, "Array in field '%s'contains records, which could not be removed due to `Restrict` strategy set", fieldDescription.Name)
			}
		}
	}
	return recordsToProcess, recordsToRemove, nil
}

func (vs *ValidationService) validateObjectsFieldArray(value interface{}, fieldDescription *meta.Field, record *Record) ([]*Record, []*Record, []*Record, error) {
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
					fieldDescription.LinkMeta.Key: pkValue,
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
			idAsString, _ := fieldDescription.LinkMeta.GetKey().ValueAsString(pkValue)
			if idAsString != "" {
				if len(beingAddedIds) != 0 {
					beingAddedIds += ","
				}
				beingAddedIds += idAsString
			}
		}
	}

	//get records which are not presented in data and should be removed from m2m relation
	if !record.IsPhantom() {
		filter := fmt.Sprintf("eq(%s,%s)", fieldDescription.Meta.Name, record.PkAsString())
		if len(beingAddedIds) > 0 {
			excludeFilter := fmt.Sprintf("not(in(%s,(%s)))", fieldDescription.LinkMeta.Name, beingAddedIds)
			filter = fmt.Sprintf("%s,%s",filter,excludeFilter)
		}
		_, recordsToRemove, _ = vs.processor.GetBulk(fieldDescription.LinkThrough.Name, filter, nil, nil, 1, true)
	}
	//get records which are already attached and remove them from list of records to process
	if !record.IsPhantom() {
		if len(beingAddedIds) > 0 {
			filter := fmt.Sprintf("eq(%s,%s),in(%s,(%s))", fieldDescription.Meta.Name, record.PkAsString(), fieldDescription.LinkMeta.Name, beingAddedIds)
			_, toExclude, _ := vs.processor.GetBulk(fieldDescription.LinkThrough.Name, filter, nil, nil, 1, true)
			for _, obj := range toExclude {
				removedCount := 0
				for i := range recordsToProcess {
					j := i - removedCount
					if recordsToProcess[j].Data[fieldDescription.LinkMeta.Name] == obj.Data[fieldDescription.LinkMeta.Name] {
						recordsToProcess = append(recordsToProcess[:j], recordsToProcess[j+1:]...)
						removedCount++
					}
				}
			}
		}
	}
	return recordsToProcess, recordsToRemove, recordsToRetrieve, nil
}

func (vs *ValidationService) validateGenericArray(value interface{}, fieldDescription *meta.Field, record *Record) ([]*Record, []*Record, error) {
	var nestedRecordsData = value.([]interface{})
	recordsToProcess := make([]*Record, len(nestedRecordsData))
	recordsToRemove := make([]*Record, 0)
	for i, recordData := range nestedRecordsData {
		if recordDataAsMap, ok := recordData.(map[string]interface{}); ok {
			recordDataAsMap[fieldDescription.OuterLinkField.Name] = &AGenericInnerLink{
				Field:           fieldDescription,
				LinkType:        meta.LinkTypeOuter,
				RecordData:      record.Data,
				Index:           i,
				NeighboursCount: len(nestedRecordsData),
				GenericInnerLink: &GenericInnerLink{
					ObjectName:       fieldDescription.Meta.Name,
					Pk:               nil,
					FieldDescription: fieldDescription,
					PkName:           fieldDescription.Meta.Key,
				},
			}
			recordsToProcess[i] = NewRecord(fieldDescription.LinkMeta, recordDataAsMap)
		} else if pkValue, ok := recordData.(interface{}); ok {
			recordsToProcess[i] = NewRecord(fieldDescription.LinkMeta, map[string]interface{}{fieldDescription.OuterLinkField.Name: &AGenericInnerLink{
				Field:           fieldDescription,
				LinkType:        meta.LinkTypeOuter,
				RecordData:      record.Data,
				Index:           i,
				NeighboursCount: len(nestedRecordsData),
				GenericInnerLink: &GenericInnerLink{
					ObjectName:       fieldDescription.Meta.Name,
					Pk:               nil,
					FieldDescription: fieldDescription,
					PkName:           fieldDescription.Meta.Key,
				},
			}, fieldDescription.LinkMeta.Key: pkValue})
		} else {
			return nil, nil, errors.NewDataError(record.Meta.Name, errors.ErrWrongFiledType, "Value in field '%s' has invalid value", fieldDescription.Name)
		}
	}

	return recordsToProcess, recordsToRemove, nil
}

func (vs *ValidationService) validateInnerGenericLink(dbTransaction transactions.DbTransaction, value interface{}, fieldDescription *meta.Field, record *Record) (*Record, error) {
	var err error
	var recordToProcess *Record
	if value != nil {
		//if value is already set by "validateGenericArray"
		if _, ok := value.(*AGenericInnerLink); ok {
			return nil, nil
		}
		if record.Data[fieldDescription.Name], err = validators.NewGenericInnerFieldValidator(dbTransaction, vs.processor.Get).Validate(fieldDescription, value); err != nil {
			if _, ok := err.(errors.GenericFieldPkIsNullError); ok {
				recordValuesAsMap := value.(map[string]interface{})
				objMeta := fieldDescription.GetLinkMetaByName(recordValuesAsMap[GenericInnerLinkObjectKey].(string))
				delete(recordValuesAsMap, GenericInnerLinkObjectKey)
				record.Data[fieldDescription.Name] = &AGenericInnerLink{
					GenericInnerLink: &GenericInnerLink{objMeta.Name, nil, fieldDescription, objMeta.Key},
					Field:            fieldDescription,
					RecordData:       recordValuesAsMap,
					Index:            0,
					NeighboursCount:  1,
					LinkType:         meta.LinkTypeInner,
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

func NewValidationService(processor *Processor) *ValidationService {
	return &ValidationService{processor: processor}
}
