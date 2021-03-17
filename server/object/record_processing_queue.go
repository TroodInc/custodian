package object

//represents list of RecordSetOperations which are queued on the same level
type RecordSetProcessingNodeLevel struct {
	RecordSets []*RecordSet
	IsRoot     bool
}
