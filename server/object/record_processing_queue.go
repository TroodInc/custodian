package object

import "custodian/server/object/record"

//represents list of RecordSetOperations which are queued on the same level
type RecordSetProcessingNodeLevel struct {
	RecordSets []*record.RecordSet
	IsRoot     bool
}
