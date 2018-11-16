package migrations

import (
	"server/migrations/operations"
	"server/object/meta"
)

type Migration struct {
	ApplyTo      *meta.Meta
	Id           string
	Predecessors []string
	Operations   []operations.MigrationOperation
}
