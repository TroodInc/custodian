package data

import (
	"server/data/record"
	"server/object/description"
	"server/object/meta"
	"fmt"
	"server/transactions"
	"server/data/errors"
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

func (r *RecordRemovalTreeBuilder) makeFilter(innerField *meta.FieldDescription, ownerId string) string {
	return fmt.Sprintf("eq(%s,%s)", innerField.Name, ownerId)
}

func (r *RecordRemovalTreeBuilder) makeGenericFilter(innerField *meta.FieldDescription, ownerObjectName string, ownerId string) string {
	return fmt.Sprintf("eq(%s.%s.%s,%s)", innerField.Name, ownerObjectName, innerField.LinkMetaList.GetByName(ownerObjectName).Key.Name, ownerId)
}

//iterate through record`s fields and process outer relations
func (r *RecordRemovalTreeBuilder) fillWithDependingRecords(recordNode *RecordRemovalNode, processor *Processor, dbTransaction transactions.DbTransaction) error {
	for _, field := range recordNode.Record.Meta.Fields {
		if field.Type == description.FieldTypeArray || (field.Type == description.FieldTypeGeneric && field.LinkType == description.LinkTypeOuter) {
			var relatedRecords []*record.Record

			pkAsString, err := recordNode.Record.Meta.Key.ValueAsString(recordNode.Record.Pk())
			if err != nil {
				return err
			}
			filter := ""
			if field.Type == description.FieldTypeArray {
				filter = r.makeFilter(field.OuterLinkField, pkAsString)
			} else if field.Type == description.FieldTypeGeneric {
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
					case description.OnDeleteCascade:
						if err := r.fillWithDependingRecords(newRecordNode, processor, dbTransaction); err != nil {
							return err
						}
						recordNode.Children[field.Name] = append(recordNode.Children[field.Name], newRecordNode)
					case description.OnDeleteRestrict:
						relatedPkAsString, _ := newRecordNode.Record.Meta.Key.ValueAsString(newRecordNode.Record.Pk())
						return errors.NewRemovalError(
							field.Meta.Name,
							fmt.Sprintf("record with PK '%s' referenced by record of '%s' with PK '%s' in strict mode", pkAsString, newRecordNode.Record.Meta.Name, relatedPkAsString),
						)
					case description.OnDeleteSetNull:
						recordNode.Children[field.Name] = append(recordNode.Children[field.Name], newRecordNode)
					case description.OnDeleteSetDefault:
						recordNode.Children[field.Name] = append(recordNode.Children[field.Name], newRecordNode)
					}
				}
			}
		}
	}
	return nil
}
