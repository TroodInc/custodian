package data

import (
	. "server/data/record"
	"server/object/meta"
)

//represents list of RecordSetOperations which are queued on the same level
type RecordProcessingNode struct {
	Record         *Record
	ProcessBefore  []*RecordProcessingNode
	ProcessAfter   []*RecordProcessingNode
	RemoveBefore   []*RecordProcessingNode
	RetrieveBefore []*RecordProcessingNode
}

//return records in order of processing
func (r *RecordProcessingNode) RecordSetOperations() (*RecordSet, []*RecordSetOperation) {
	recordOperations := r.collectRecordOperations(make([]*RecordOperation, 0))
	return r.composeRecordSetOperations(recordOperations)
}

func (r *RecordProcessingNode) collectRecordOperations(recordOperations []*RecordOperation) []*RecordOperation {
	for _, recordProcessingNode := range r.RetrieveBefore {
		recordOperations = append(
			recordOperations,
			&RecordOperation{Record: recordProcessingNode.Record, Type: RecordOperationTypeRetrieve},
		)
	}

	for _, recordProcessingNode := range r.RemoveBefore {
		recordOperations = append(
			recordOperations,
			&RecordOperation{Record: recordProcessingNode.Record, Type: RecordOperationTypeRemove},
		)
	}

	for _, recordProcessingNode := range r.ProcessBefore {
		recordOperations = recordProcessingNode.collectRecordOperations(recordOperations)
	}

	var operation RecordOperationType
	if r.Record.IsPhantom() {
		operation = RecordOperationTypeCreate
	} else {
		operation = RecordOperationTypeUpdate
	}
	recordOperations = append(recordOperations, &RecordOperation{Record: r.Record, Type: operation})

	for _, recordProcessingNode := range r.ProcessAfter {
		recordOperations = recordProcessingNode.collectRecordOperations(recordOperations)
	}
	return recordOperations
}

//unite records of same objects, so they could be processed within same DB operation
func (r *RecordProcessingNode) composeRecordSetOperations(recordOperations []*RecordOperation) (*RecordSet, []*RecordSetOperation) {
	recordSetOperations := make([]*RecordSetOperation, 0)
	rootRecordSet := new(RecordSet)
	var currentMeta *meta.Meta
	var currentOperationType RecordOperationType
	var currentMetaRecordSetOperationPool []*RecordSetOperation //pool represent a set of recordSetOperations, which belong to the same
	// object, but should be processed separately dut to different sets of fields
	var currentRecordSetOperation *RecordSetOperation
	for _, currentRecordOperation := range recordOperations {
		//change pool and meta if needed
		isRootRecord := r.Record == currentRecordOperation.Record
		metaChanged := currentMeta == nil || currentMeta.Name != currentRecordOperation.Record.Meta.Name
		operationChanged := currentRecordOperation.Type != currentOperationType
		if currentMeta == nil || isRootRecord || metaChanged || operationChanged {
			currentMeta = currentRecordOperation.Record.Meta
			currentOperationType = currentRecordOperation.Type
			currentMetaRecordSetOperationPool = make([]*RecordSetOperation, 0)
		}

		//find matching recordSet for current record
		currentRecordSetOperation = nil
		for _, recordSetOperation := range currentMetaRecordSetOperationPool {
			if recordSetOperation.RecordSet.CanAppendRecord(currentRecordOperation.Record) {
				currentRecordSetOperation = recordSetOperation
			}
		}
		//if record set is not found - create a new one
		if currentRecordSetOperation == nil {
			currentRecordSetOperation = &RecordSetOperation{RecordSet: NewRecordSet(currentRecordOperation.Record.Meta), Type: currentRecordOperation.Type}
			currentMetaRecordSetOperationPool = append(currentMetaRecordSetOperationPool, currentRecordSetOperation)
			recordSetOperations = append(recordSetOperations, currentRecordSetOperation)
		}

		//root record`s recordSet should contain only root record
		if isRootRecord {
			rootRecordSet = currentRecordSetOperation.RecordSet
		}
		currentRecordSetOperation.RecordSet.Records = append(currentRecordSetOperation.RecordSet.Records, currentRecordOperation.Record)
	}
	return rootRecordSet, recordSetOperations
}

func NewRecordProcessingNode(record *Record) *RecordProcessingNode {
	return &RecordProcessingNode{Record: record}
}
