package data

import (
	"server/data/record"
	"server/object/meta"
)

//represents list of RecordSets which are queued on the same level
type RecordProcessingNode struct {
	Record        *record.Record
	ProcessBefore []*RecordProcessingNode
	ProcessAfter  []*RecordProcessingNode
}

//return records in order of processing
func (r *RecordProcessingNode) RecordSets() (*record.RecordSet, []*record.RecordSet) {
	records := r.collectRecords(make([]*record.Record, 0))
	return r.composeRecordSets(records)
}

func (r *RecordProcessingNode) collectRecords(records []*record.Record) []*record.Record {
	for _, recordProcessingNode := range r.ProcessBefore {
		records = recordProcessingNode.collectRecords(records)
	}
	records = append(records, r.Record)
	for _, recordProcessingNode := range r.ProcessAfter {
		records = recordProcessingNode.collectRecords(records)
	}
	return records
}

//unite records of same objects, so they could be processed within same DB operation
func (r *RecordProcessingNode) composeRecordSets(records []*record.Record) (*record.RecordSet, []*record.RecordSet) {
	recordSets := make([]*record.RecordSet, 0)
	rootRecordSet := new(record.RecordSet)
	var currentMeta *meta.Meta
	var currentMetaRecordSetPool []*record.RecordSet //pool represent a set of recordSets, which belong to the same
	// object, but should be processed separately dut to different sets of fields
	var currentRecordSet *record.RecordSet
	for _, currentRecord := range records {
		//change pool and meta if needed
		if currentMeta == nil || r.Record == currentRecord || currentMeta.Name != currentRecord.Meta.Name {
			currentMeta = currentRecord.Meta
			currentMetaRecordSetPool = make([]*record.RecordSet, 0)

		}

		//find matching recordSet for current record
		currentRecordSet = nil
		for _, recordSet := range currentMetaRecordSetPool {
			if recordSet.CanAppendRecord(currentRecord) {
				currentRecordSet = recordSet
			}
		}
		//if record set is not found - create a new one
		if currentRecordSet == nil {
			currentRecordSet = record.NewRecordSet(currentRecord.Meta)
			currentMetaRecordSetPool = append(currentMetaRecordSetPool, currentRecordSet)
			recordSets = append(recordSets, currentRecordSet)
		}

		//root record`s recordSet should contain only root record
		if r.Record == currentRecord {
			rootRecordSet = currentRecordSet
		}
		currentRecordSet.Records = append(currentRecordSet.Records, currentRecord)
	}
	return rootRecordSet, recordSets
}

func NewRecordProcessingNode(record *record.Record) *RecordProcessingNode {
	return &RecordProcessingNode{Record: record}
}
