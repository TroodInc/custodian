package migrations

import (
	"server/object/meta"
	"server/transactions"
	"server/pg/migrations/operations"
	"server/migrations/description"
	"server/migrations/migrations"
)

type MigrationFactory struct {
	metaStore             *meta.MetaStore
	metaDescriptionSyncer meta.MetaDescriptionSyncer
	globalTransaction     *transactions.GlobalTransaction
}

func (mf *MigrationFactory) Factory(migrationDescription *description.MigrationDescription) (*migrations.Migration, error) {
	migration := &migrations.Migration{MigrationDescription: *migrationDescription}

	if migration.MigrationDescription.ApplyTo != "" {
		if applyTo, _, err := mf.metaStore.Get(mf.globalTransaction, migration.MigrationDescription.ApplyTo, false); err != nil {
			return nil, err
		} else {
			migration.ApplyTo = applyTo
		}
	}

	operationFactory := operations.NewOperationFactory(meta.NewMetaFactory(mf.metaDescriptionSyncer))

	for i := range migration.MigrationDescription.Operations {
		operation, err := operationFactory.Factory(&migration.MigrationDescription.Operations[i], migration.ApplyTo)
		if err != nil {
			return nil, err
		}

		migration.Operations = append(migration.Operations, operation)
	}
	return migration, nil
}

func NewMigrationFactory(metaStore *meta.MetaStore, globalTransaction *transactions.GlobalTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) *MigrationFactory {
	return &MigrationFactory{metaStore: metaStore, globalTransaction: globalTransaction, metaDescriptionSyncer: metaDescriptionSyncer}
}
