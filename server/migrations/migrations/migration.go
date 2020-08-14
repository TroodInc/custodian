package migrations

import (
	meta_description "custodian/server/object/description"
	"custodian/server/migrations/description"
	"custodian/server/migrations/operations"
)

const (
	MigrationDirectionForward  = "forward"
	MigrationDirectionBackward = "backward"
)

type Migration struct {
	description.MigrationDescription
	ApplyTo    *meta_description.MetaDescription
	Operations []operations.MigrationOperation
	RunBefore  []*description.MigrationDescription //Migrations which are spawned by the owner, used to store \
	RunAfter   []*description.MigrationDescription //automatically generated migrations(eg outer links handling)
	Direction  string
}

func (m *Migration) IsForward() bool {
	return m.Direction == MigrationDirectionForward
}
