package migrations

import (
	"server/migrations/description"
	"server/object/meta"
	pg_operations "server/pg/migrations/operations"
)

type MigrationFactory struct {
	normalizationMigrationsFactory *description.NormalizationMigrationsFactory
}

func (mf *MigrationFactory) FactoryForward(md *description.MigrationDescription, metaObj *meta.Meta) (*Migration, error) {
	if migration, err := mf.factory(md, metaObj); err != nil {
		return nil, err
	} else {
		migration.Direction = MigrationDirectionForward
		return migration, nil
	}
}

func (mf *MigrationFactory) FactoryBackward(md *description.MigrationDescription, metaObj *meta.Meta) (*Migration, error) {
	if migration, err := mf.factory(md, metaObj); err != nil {
		return nil, err
	} else {
		migration.Direction = MigrationDirectionBackward
		return migration, nil
	}
}

func (mf *MigrationFactory) factory(migrationDescription *description.MigrationDescription, metaObj *meta.Meta) (*Migration, error) {
	migration := &Migration{MigrationDescription: *migrationDescription, ApplyTo: metaObj}

	operationFactory := pg_operations.NewOperationFactory()

	for i := range migration.MigrationDescription.Operations {
		operation, err := operationFactory.Factory(&migration.MigrationDescription, &migration.MigrationDescription.Operations[i], migration.ApplyTo)
		if err != nil {
			return nil, err
		}

		migration.Operations = append(migration.Operations, operation)

		runBefore, runAfter, err := mf.normalizationMigrationsFactory.Factory(migration.ApplyTo, operation)
		if err != nil {
			return nil, err
		}
		if len(runBefore) > 0 {
			migration.RunBefore = append(migration.RunBefore, runBefore...)
		}
		if len(runAfter) > 0 {
			migration.RunAfter = append(migration.RunAfter, runAfter...)
		}
	}
	return migration, nil
}

func NewMigrationFactory() *MigrationFactory {
	return &MigrationFactory{

		//normalizationMigrationsFactory: description.NewNormalizationMigrationsFactory(metaDescriptionSyncer)
	}
}
