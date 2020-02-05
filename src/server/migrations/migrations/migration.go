package migrations

import (
	"server/migrations/description"
	"server/migrations/operations"
	"server/object/meta"
)

const (
	MigrationDirectionForward  = "forward"
	MigrationDirectionBackward = "backward"
)

type Migration struct {
	description.MigrationDescription
	ApplyTo    *meta.Meta
	Operations []operations.MigrationOperation
	RunBefore  []*description.MigrationDescription //Migrations which are spawned by the owner, used to store \
	RunAfter   []*description.MigrationDescription //automatically generated migrations(eg outer links handling)
	Direction  string
}

func (m *Migration) IsForward() bool {
	return m.Direction == MigrationDirectionForward
}
