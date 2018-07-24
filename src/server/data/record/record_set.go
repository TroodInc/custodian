package record

import (
	"server/meta"
	"utils"
)

type RecordSet struct {
	Meta    *meta.Meta
	DataSet []map[string]interface{}
}

func (recordSet *RecordSet) CollapseLinks() {
	for _, recordData := range recordSet.DataSet {
		collapseLinks(recordData)
	}
}

func (recordSet *RecordSet) GetRecordData(id interface{}) map[string]interface{} {
	for _, recordData := range recordSet.DataSet {
		if recordData[recordSet.Meta.Key.Name] == id {
			return recordData
		}
	}
	return nil
}

func (recordSet *RecordSet) Clone() *RecordSet {
	clonedRecordSet := RecordSet{Meta: recordSet.Meta, DataSet: make([]map[string]interface{}, len(recordSet.DataSet))}
	for i, data := range recordSet.DataSet {
		clonedRecordSet.DataSet[i] = utils.CloneMap(data)
	}
	return &clonedRecordSet
}
