package data

import (
	"server/data/record"
	"server/object/meta"
)

//Represents record and its dependent children
type RecordRemovalNode struct {
	Children         map[string][]*RecordRemovalNode
	Parent           *RecordRemovalNode
	LinkField        *meta.Field
	Record           *record.Record
	OnDeleteStrategy *meta.OnDeleteStrategy
}

func NewRecordRemovalNode(record *record.Record, onDeleteStrategy *meta.OnDeleteStrategy, parent *RecordRemovalNode, linkField *meta.Field) *RecordRemovalNode {
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
		var onDeleteStrategy meta.OnDeleteStrategy
		if len(childNodes) > 0 {
			onDeleteStrategy = *childNodes[0].OnDeleteStrategy
		}
		switch onDeleteStrategy {
		case meta.OnDeleteCascade:
			childrenData := make([]interface{}, 0)
			for _, childNode := range childNodes {
				childrenData = append(childrenData, r.appendChildNodes(childNode.Record.Data, childNode.Children))
				data[childName] = childrenData
			}
		case meta.OnDeleteSetNull:
			delete(data, childName)
		}
	}
	return data
}
