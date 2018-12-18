package migrations

import (
	"server/object/meta"
	"server/transactions"
	"server/migrations/migrations"
)

type MigrationRunner struct {
	metaDescriptionSyncer meta.MetaDescriptionSyncer
}

func (mr *MigrationRunner) Run(migration *migrations.Migration, globalTransaction *transactions.GlobalTransaction) (updatedMeta *meta.Meta, err error) {
	if err := mr.validateMigration(migration); err != nil {
		return nil, err
	}

	for _, operation := range migration.Operations {
		updatedMeta, err = operation.SyncMetaDescription(migration.ApplyTo, globalTransaction.MetaDescriptionTransaction, mr.metaDescriptionSyncer)
		if err != nil {
			return nil, err
		} else {
			err := operation.SyncDbDescription(updatedMeta, globalTransaction.DbTransaction)
			if err != nil {
				return nil, err
			}
		}
	}

	return updatedMeta, nil
}

//TODO: Implement checks:
//1. Migration's predecessors has been applied
//2. Migration has not been applied yet
func (mr *MigrationRunner) validateMigration(migration *migrations.Migration) error {
	return nil
}

//TODO: Implement checks:
//1. Migration's predecessors has been applied
//2. Migration has not been applied yet
func (mr *MigrationRunner) recordAppliedMigration(migration *migrations.Migration, transaction transactions.DbTransaction) error {
	return nil
}
