package data

import (
	"server/data/record"
	"server/object/description"
	"server/object/meta"
	"fmt"
	"server/transactions"
	"server/data/errors"
)

type RecordNode struct {
	Children         map[string][]*RecordNode
	Parent           *RecordNode
	LinkField        *meta.FieldDescription
	Record           *record.Record
	OnDeleteStrategy *description.OnDeleteStrategy
}

func NewRecordNode(record *record.Record, onDeleteStrategy *description.OnDeleteStrategy, parent *RecordNode, linkField *meta.FieldDescription) *RecordNode {
	return &RecordNode{
		Record:           record,
		Children:         make(map[string][]*RecordNode),
		Parent:           parent,
		LinkField:        linkField,
		OnDeleteStrategy: onDeleteStrategy,
	}
}

type RecordRemovalTreeExtractor struct {
}

//Extract record`s full tree consisting of depending records, which would be affected by root record removal
func (r *RecordRemovalTreeExtractor) Extract(record *record.Record, processor *Processor, dbTransaction transactions.DbTransaction) (*RecordNode, error) {
	recordTree := NewRecordNode(record, nil, nil, nil)
	if err := r.fillWithDependingRecords(recordTree, processor, dbTransaction); err != nil {
		return nil, err
	} else {
		return recordTree, nil
	}
}

func (r *RecordRemovalTreeExtractor) makeFilter(innerField *meta.FieldDescription, ownerId string) string {
	return fmt.Sprintf("eq(%s,%s)", innerField.Name, ownerId)
}

//iterate through record`s fields and process outer relations
func (r *RecordRemovalTreeExtractor) fillWithDependingRecords(recordNode *RecordNode, processor *Processor, dbTransaction transactions.DbTransaction) error {
	for _, field := range recordNode.Record.Meta.Fields {
		if field.Type == description.FieldTypeArray || (field.Type == description.FieldTypeGeneric && field.LinkType == description.LinkTypeOuter) {
			relatedRecords := make([]map[string]interface{}, 0)
			callbackFunction := func(obj map[string]interface{}) error {
				relatedRecords = append(relatedRecords, obj)
				return nil
			}

			pkAsString, err := recordNode.Record.Meta.Key.ValueAsString(recordNode.Record.Pk())
			if err != nil {
				return err
			}
			err = processor.GetBulk(dbTransaction, field.LinkMeta.Name, r.makeFilter(field.OuterLinkField, pkAsString), 1, callbackFunction)
			if err != nil {
				return err
			}

			if len(relatedRecords) > 0 {
				recordNode.Children[field.Name] = make([]*RecordNode, 0)
				for _, relatedRecord := range relatedRecords {
					newRecordNode := NewRecordNode(
						record.NewRecord(field.LinkMeta, relatedRecord),
						&field.OuterLinkField.OnDelete,
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
