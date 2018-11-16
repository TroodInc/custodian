package object

import "server/migrations/operations"

type UpdateObjectOperation struct {
	FieldOperations []*operations.MigrationOperation
}

func (o *UpdateObjectOperation) Sql() []string {
	return []string{""}
}
