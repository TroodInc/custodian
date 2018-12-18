package migrations

import (
	"server/object/meta"
	"server/migrations/operations"
	"server/migrations/description"
)

type Migration struct {
	description.MigrationDescription
	ApplyTo    *meta.Meta
	Operations []operations.MigrationOperation
}
