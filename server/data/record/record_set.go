package record

import (
	"custodian/server/object/meta"
	"custodian/utils"
)

type RecordSet struct {
	Meta    *meta.Meta
	Records []*Record
}

func (recordSet *RecordSet) CollapseLinks() {
	for _, record := range recordSet.Records {
		record.CollapseLinks()
	}
}

func (recordSet *RecordSet) IsPhantom() bool {
	if len(recordSet.Records) > 0 {
		return recordSet.Records[0].IsPhantom()
	}
	return true
}

func (recordSet *RecordSet) PrepareData(operationType RecordOperationType) {
	for _, record := range recordSet.Records {
		record.PrepareData(operationType)
	}
}

func (recordSet *RecordSet) RawData() []map[string]interface{} {
	rawData := make([]map[string]interface{}, len(recordSet.Records))
	for i, record := range recordSet.Records {
		rawData[i] = record.RawData
	}
	return rawData
}

func (recordSet *RecordSet) Data() []map[string]interface{} {
	Data := make([]map[string]interface{}, len(recordSet.Records))
	for i, record := range recordSet.Records {
		Data[i] = record.Data
	}
	return Data
}

func (recordSet *RecordSet) MergeData() {
	for _, record := range recordSet.Records {
		record.MergeData()
	}
}

func (recordSet *RecordSet) GetRecordById(id interface{}) *Record {
	for _, record := range recordSet.Records {
		if record.Data[recordSet.Meta.Key.Name] == id {
			return record
		}
	}
	return nil
}

// return true if record could be appended to this recordRet, i.e record has the same set of fields and is of the same object
func (recordSet *RecordSet) CanAppendRecord(record *Record) bool {
	if record.Meta.Name == recordSet.Meta.Name {
		if len(recordSet.Records) == 0 {
			return true
		} else {
			recordSetKeys, _ := utils.GetMapKeysValues(recordSet.Records[0].Data)
			recordKeys, _ := utils.GetMapKeysValues(record.Data)
			return utils.Equal(recordSetKeys, recordKeys, false)
		}
	} else {
		return false
	}

}

func NewRecordSet(meta *meta.Meta) *RecordSet {
	return &RecordSet{Meta: meta, Records: make([]*Record, 0)}
}
