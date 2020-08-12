package record

type RecordOperationType int

const (
	RecordOperationTypeCreate   RecordOperationType = iota + 1
	RecordOperationTypeUpdate
	RecordOperationTypeRemove
	RecordOperationTypeRetrieve
)

type RecordOperation struct {
	Record *Record
	Type   RecordOperationType
}

type RecordSetOperation struct {
	RecordSet *RecordSet
	Type      RecordOperationType
}
