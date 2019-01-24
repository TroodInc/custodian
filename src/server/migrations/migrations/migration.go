package migrations

import (
	meta_description "server/object/description"
	"server/migrations/description"
	"server/migrations/operations"
)

type Migration struct {
	description.MigrationDescription
	ApplyTo    *meta_description.MetaDescription
	Operations []operations.MigrationOperation
	RunBefore  []*description.MigrationDescription //Migrations which are spawned by the owner, used to store \
	RunAfter   []*description.MigrationDescription //automatically generated migrations(eg outer links handling)
}
