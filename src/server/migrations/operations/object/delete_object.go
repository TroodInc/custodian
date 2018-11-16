package object

import "server/migrations/operations"

type DeleteObjectOperation struct {
	FieldOperations []*operations.MigrationOperation
}

func (o *DeleteObjectOperation) Sql() string {
	return ""
}
