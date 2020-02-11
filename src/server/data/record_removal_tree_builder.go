package data

import (
	"fmt"
	"server/data/errors"
	"server/data/record"
	"server/object/meta"
	"server/transactions"
)

type RecordRemovalTreeBuilder struct {
}

//Extract record`s full tree consisting of depending records, which would be affected by root record removal
func (r *RecordRemovalTreeBuilder) Extract(record *record.Record, processor *Processor, dbTransaction transactions.DbTransaction) (*RecordRemovalNode, error) {
	recordTree := NewRecordRemovalNode(record, nil, nil, nil)
	if err := r.fillWithDependingRecords(recordTree, processor, dbTransaction); err != nil {
		return nil, err
	} else {
		return recordTree, nil
	}
}

func (r *RecordRemovalTreeBuilder) makeFilter(innerField *meta.Field, ownerId string) string {
	return fmt.Sprintf("eq(%s,%s)", innerField.Name, ownerId)
}

func (r *RecordRemovalTreeBuilder) makeGenericFilter(innerField *meta.Field, ownerObjectName string, ownerId string) string {
	return fmt.Sprintf("eq(%s.%s.%s,%s)", innerField.Name, ownerObjectName, innerField.GetLinkMetaByName(ownerObjectName).Key, ownerId)
}

//iterate through record`s fields and process outer relations
func (r *RecordRemovalTreeBuilder) fillWithDependingRecords(recordNode *RecordRemovalNode, processor *Processor, dbTransaction transactions.DbTransaction) error {
	for _, field := range recordNode.Record.Meta.Fields {
		if field.Type == meta.FieldTypeArray || (field.Type == meta.FieldTypeGeneric && field.LinkType == meta.LinkTypeOuter) {
			var relatedRecords []*record.Record

			pkAsString, err := recordNode.Record.Meta.GetKey().ValueAsString(recordNode.Record.Pk())
			if err != nil {
				return err
			}
			filter := ""
			if field.Type == meta.FieldTypeArray {
				filter = r.makeFilter(field.OuterLinkField, pkAsString)
			} else if field.Type == meta.FieldTypeGeneric {
				filter = r.makeGenericFilter(field.OuterLinkField, recordNode.Record.Meta.Name, pkAsString)
			}
			_, relatedRecords, err = processor.GetBulk(field.LinkMeta.Name, filter, nil, nil, 1, false)
			if err != nil {
				return err
			}

			if len(relatedRecords) > 0 {
				recordNode.Children[field.Name] = make([]*RecordRemovalNode, 0)
				for _, relatedRecord := range relatedRecords {
					newRecordNode := NewRecordRemovalNode(
						record.NewRecord(field.LinkMeta, relatedRecord.Data),
						field.OuterLinkField.OnDeleteStrategy(),
						recordNode,
						field.OuterLinkField,
					)
					switch *newRecordNode.OnDeleteStrategy {
					case meta.OnDeleteCascade:
						if err := r.fillWithDependingRecords(newRecordNode, processor, dbTransaction); err != nil {
							return err
						}
						recordNode.Children[field.Name] = append(recordNode.Children[field.Name], newRecordNode)
					case meta.OnDeleteRestrict:
						relatedPkAsString, _ := newRecordNode.Record.Meta.GetKey().ValueAsString(newRecordNode.Record.Pk())
						return errors.NewRemovalError(
							field.Meta.Name,
							fmt.Sprintf("record with PK '%s' referenced by record of '%s' with PK '%s' in strict mode", pkAsString, newRecordNode.Record.Meta.Name, relatedPkAsString),
						)
					case meta.OnDeleteSetNull:
						recordNode.Children[field.Name] = append(recordNode.Children[field.Name], newRecordNode)
					case meta.OnDeleteSetDefault:
						recordNode.Children[field.Name] = append(recordNode.Children[field.Name], newRecordNode)
					}
				}
			}
		}
	}
	return nil
}
