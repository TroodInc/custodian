package migrations

import (
	"server/migrations/description"
	"server/object/meta"
	pg_operations "server/pg/migrations/operations"
)

type MigrationFactory struct {
	metaDescriptionSyncer          meta.MetaDescriptionSyncer
	normalizationMigrationsFactory *description.NormalizationMigrationsFactory
}

func (mf *MigrationFactory) FactoryForward(migrationDescription *description.MigrationDescription) (*Migration, error) {
	if migration, err := mf.factory(migrationDescription); err != nil {
		return nil, err
	} else {
		migration.Direction = MigrationDirectionForward
		return migration, nil
	}
}

func (mf *MigrationFactory) FactoryBackward(migrationDescription *description.MigrationDescription) (*Migration, error) {
	if migration, err := mf.factory(migrationDescription); err != nil {
		return nil, err
	} else {
		migration.Direction = MigrationDirectionBackward
		return migration, nil
	}
}

func (mf *MigrationFactory) factory(migrationDescription *description.MigrationDescription) (*Migration, error) {
	migration := &Migration{MigrationDescription: *migrationDescription}

	if migration.MigrationDescription.ApplyTo != "" {
		if applyTo, _, err := mf.metaDescriptionSyncer.Get(migration.MigrationDescription.ApplyTo); err != nil {
			return nil, err
		} else {
			migration.ApplyTo = meta.NewMetaFromMap(applyTo)
		}
	}

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

func NewMigrationFactory(metaDescriptionSyncer meta.MetaDescriptionSyncer) *MigrationFactory {
	return &MigrationFactory{metaDescriptionSyncer: metaDescriptionSyncer, normalizationMigrationsFactory: description.NewNormalizationMigrationsFactory(metaDescriptionSyncer)}
}
