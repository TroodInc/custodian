package data

import "server/data/record"

type RecordOperationType int

const (
	RecordOperationTypeCreate  RecordOperationType = iota + 1
	RecordOperationTypeUpdate
	RecordOperationTypeRemove
	RecordOperationTypeRetrive
)

type RecordOperation struct {
	Record *record.Record
	Type   RecordOperationType
}

type RecordSetOperation struct {
	RecordSet *record.RecordSet
	Type      RecordOperationType
}
