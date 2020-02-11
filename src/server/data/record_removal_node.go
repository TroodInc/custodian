package data

import (
	"server/data/record"
	"server/object"
)

//Represents record and its dependent children
type RecordRemovalNode struct {
	Children         map[string][]*RecordRemovalNode
	Parent           *RecordRemovalNode
	LinkField        *object.Field
	Record           *record.Record
	OnDeleteStrategy *object.OnDeleteStrategy
}

func NewRecordRemovalNode(record *record.Record, onDeleteStrategy *object.OnDeleteStrategy, parent *RecordRemovalNode, linkField *object.Field) *RecordRemovalNode {
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
		var onDeleteStrategy object.OnDeleteStrategy
		if len(childNodes) > 0 {
			onDeleteStrategy = *childNodes[0].OnDeleteStrategy
		}
		switch onDeleteStrategy {
		case object.OnDeleteCascade:
			childrenData := make([]interface{}, 0)
			for _, childNode := range childNodes {
				childrenData = append(childrenData, r.appendChildNodes(childNode.Record.Data, childNode.Children))
				data[childName] = childrenData
			}
		case object.OnDeleteSetNull:
			delete(data, childName)
		}
	}
	return data
}
