package record

import "server/meta"

type RecordSet struct {
	Meta    *meta.Meta
	DataSet []map[string]interface{}
}

func (recordSet *RecordSet) CollapseLinks() {
	for _, recordData := range recordSet.DataSet {
		collapseLinks(recordData)
	}
}
