package data

import (
	"custodian/server/data/record"
)

type RecordProcessingTreeBuilder struct {
}

//Build record processing tree where each child node represents related record
func (r *RecordProcessingTreeBuilder) Build(record *record.Record, processor *Processor) (*RecordProcessingNode, error) {
	rootProcessingNode := NewRecordProcessingNode(record)
	if err := r.buildNode(rootProcessingNode, processor); err != nil {
		return nil, err
	} else {
		return rootProcessingNode, nil
	}
}

func (r *RecordProcessingTreeBuilder) buildNode(recordProcessingNode *RecordProcessingNode, processor *Processor) error {
	var err error
	if recordProcessingNode.RetrieveBefore, recordProcessingNode.ProcessBefore, recordProcessingNode.ProcessAfter, recordProcessingNode.RemoveBefore, err = NewValidationService(processor.metaStore, processor).Validate(recordProcessingNode.Record); err != nil {
		return err
	} else {
		for _, childRecordProcessingNode := range recordProcessingNode.ProcessBefore {
			if err = r.buildNode(childRecordProcessingNode, processor); err != nil {
				return err
			}
		}
		for _, childRecordProcessingNode := range recordProcessingNode.ProcessAfter {
			if err = r.buildNode(childRecordProcessingNode, processor); err != nil {
				return err
			}
		}
	}
	return nil
}
