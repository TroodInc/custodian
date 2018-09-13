package data

import (
	"server/data/record"
	"server/transactions"
)

type RecordProcessingTreeBuilder struct {
}

//Build record processing tree where each child node represents related record
func (r *RecordProcessingTreeBuilder) Build(record *record.Record, processor *Processor, dbTransaction transactions.DbTransaction) (*RecordProcessingNode, error) {
	rootProcessingNode := NewRecordProcessingNode(record)
	if err := r.buildNode(rootProcessingNode, processor, dbTransaction); err != nil {
		return nil, err
	} else {
		return rootProcessingNode, nil
	}
}

func (r *RecordProcessingTreeBuilder) buildNode(recordProcessingNode *RecordProcessingNode, processor *Processor, dbTransaction transactions.DbTransaction) error {
	var err error
	if recordProcessingNode.ProcessBefore, recordProcessingNode.ProcessAfter, err = NewValidationService(processor.metaStore, processor).Validate(dbTransaction, recordProcessingNode.Record); err != nil {
		return err
	} else {
		for _, childRecordProcessingNode := range recordProcessingNode.ProcessBefore {
			r.buildNode(childRecordProcessingNode, processor, dbTransaction)
		}
		for _, childRecordProcessingNode := range recordProcessingNode.ProcessAfter {
			r.buildNode(childRecordProcessingNode, processor, dbTransaction)
		}
	}
	return nil
}
