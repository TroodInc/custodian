package data

import (
	"custodian/server/object/meta"
	"custodian/server/data/record"
	"custodian/server/object/description"
)

//Represents record and its dependent children
type RecordRemovalNode struct {
	Children         map[string][]*RecordRemovalNode
	Parent           *RecordRemovalNode
	LinkField        *meta.FieldDescription
	Record           *record.Record
	OnDeleteStrategy *description.OnDeleteStrategy
}

func NewRecordRemovalNode(record *record.Record, onDeleteStrategy *description.OnDeleteStrategy, parent *RecordRemovalNode, linkField *meta.FieldDescription) *RecordRemovalNode {
	return &RecordRemovalNode{
		Record:           record,
		Children:         make(map[string][]*RecordRemovalNode),
		Parent:           parent,
		LinkField:        linkField,
		OnDeleteStrategy: onDeleteStrategy,
	}
}

//Get record`s data and children`s data, which is directly bind to parent record
func (r *RecordRemovalNode) Data() map[string]interface{} {
	return r.appendChildNodes(r.Record.Data, r.Children).(map[string]interface{})
}

func (r *RecordRemovalNode) appendChildNodes(data map[string]interface{}, children map[string][]*RecordRemovalNode) interface{} {
	for childName, childNodes := range children {
		var onDeleteStrategy description.OnDeleteStrategy
		if len(childNodes) > 0 {
			onDeleteStrategy = *childNodes[0].OnDeleteStrategy
		}
		switch onDeleteStrategy {
		case description.OnDeleteCascade:
			childrenData := make([]interface{}, 0)
			for _, childNode := range childNodes {
				childrenData = append(childrenData, r.appendChildNodes(childNode.Record.Data, childNode.Children))
				data[childName] = childrenData
			}
		case description.OnDeleteSetNull:
			delete(data, childName)
		}
	}
	return data
}
