package data

import "custodian/server/data/record"

//represents list of RecordSetOperations which are queued on the same level
type RecordSetProcessingNodeLevel struct {
	RecordSets []*record.RecordSet
	IsRoot     bool
}
