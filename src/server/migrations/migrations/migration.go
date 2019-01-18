package migrations

import (
	meta_description "server/object/description"
	"server/migrations/description"
	"server/migrations/operations"
	"server/migrations"
)

type Migration struct {
	description.MigrationDescription
	ApplyTo    *meta_description.MetaDescription
	Operations []operations.MigrationOperation
	RunBefore  []*description.MigrationDescription //Migrations which are spawned by the owner, used to store \
	RunAfter   []*description.MigrationDescription //automatically generated migrations(eg outer links handling)
}

//Returns meta`s name which this migration is intended for
//TODO: implement MigrationDescription`s validation in a separate step
func (m *Migration) MetaName() (string, error) {
	if m.MigrationDescription.ApplyTo != "" {
		return m.MigrationDescription.ApplyTo, nil
	} else {
		if m.MigrationDescription.Operations[0].Type != description.CreateObjectOperation {
			return "", migrations.NewMigrationError(migrations.MigrationErrorInvalidDescription, "Migration has neither ApplyTo defined nor createObject operation")
		}
		return m.MigrationDescription.Operations[0].MetaDescription.Name, nil
	}
}
