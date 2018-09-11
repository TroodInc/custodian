package record

import (
	"server/object/meta"
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
