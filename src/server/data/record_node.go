package data

import (
	"server/object/meta"
	"server/data/record"
	"server/object/description"
)

//Represents record and its dependent children
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

//Get record`s data and children`s data, which is directly bind to parent record
func (r *RecordNode) Data() map[string]interface{} {
	return r.appendChildNodes(r.Record.Data, r.Children).(map[string]interface{})
}

func (r *RecordNode) appendChildNodes(data map[string]interface{}, children map[string][]*RecordNode) interface{} {
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
