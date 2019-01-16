package managers

import (
	"server/transactions"
	"server/pg"
	"database/sql"
	"server/object/meta"
	"server/object/description"
	"server/pg/migrations/operations/object"
	"server/migrations/migrations"
	_migrations "server/migrations"
	"fmt"
	migrations_description "server/migrations/description"
)

const historyMetaName = "__custodian_objects_migration_history__"

type MigrationManager struct {
	metaStore             *meta.MetaStore
	dataManager           *pg.DataManager
	metaDescriptionSyncer meta.MetaDescriptionSyncer
}

func (mm *MigrationManager) recordAppliedMigration(migration *migrations.Migration, transaction transactions.DbTransaction) (string, error) {
	historyMeta, err := mm.ensureHistoryTableExists(transaction)
	if err != nil {
		return "", err
	}

	err = mm.canApplyMigration(migration, transaction)
	if err != nil {
		return "", err
	}

	migrationData := map[string]interface{}{"migration_id": migration.Id}
	operation, err := mm.dataManager.PrepareCreateOperation(historyMeta, []map[string]interface{}{migrationData})
	if err != nil {
		return "", err
	}

	operations := []transactions.Operation{operation}

	if e := transaction.Execute(operations); e != nil {
		return "", e
	}

	migrationIdStrValue, _ := historyMeta.Key.ValueAsString(migrationData[historyMeta.Key.Name])
	return migrationIdStrValue, nil
}

func (mm *MigrationManager) ensureHistoryTableExists(transaction transactions.DbTransaction) (*meta.Meta, error) {
	_, err := pg.MetaDDLFromDB(transaction.Transaction().(*sql.Tx), historyMetaName)
	doesNotExist := false
	if err != nil {
		switch castError := err.(type) {
		case *pg.DDLError:
			if castError.Code() == pg.ErrNotFound {
				doesNotExist = true
			} else {
				return nil, err
			}
		default:
			return nil, err
		}
	}

	historyMeta, err := mm.factoryHistoryMeta()
	if err != nil {
		return nil, err
	}

	if doesNotExist {
		if err = object.NewCreateObjectOperation(historyMeta.MetaDescription).SyncDbDescription(nil, transaction, mm.metaDescriptionSyncer); err != nil {
			return nil, err
		}
	}
	return historyMeta, nil
}

func (mm *MigrationManager) canApplyMigration(migration *migrations.Migration, transaction transactions.DbTransaction) error {
	historyMeta, err := mm.ensureHistoryTableExists(transaction)
	if err != nil {
		return err
	}

	filters := map[string]interface{}{"migration_id": migration.Id}
	fields := []*meta.FieldDescription{historyMeta.Key}
	result, err := mm.dataManager.GetAll(historyMeta, fields, filters, transaction)
	if err != nil {
		return nil
	}
	if len(result) == 0 {
		return nil
	} else {
		return _migrations.NewMigrationError(
			fmt.Sprintf("Migration with ID '%s' has already been applied", migration.Id),
		)
	}

}

func (mm *MigrationManager) factoryHistoryMeta() (*meta.Meta, error) {
	historyMetaDescription := &description.MetaDescription{
		Name: historyMetaName,
		Key:  "id",
		Fields: []description.Field{
			{
				Name:     "id",
				Type:     description.FieldTypeNumber,
				Optional: true,
				Def: map[string]interface{}{
					"func": "nextval",
				},
			},
			{
				Name:     "migration_id",
				Type:     description.FieldTypeString,
				Optional: false,
			},
			{
				Name:     "created",
				Type:     description.FieldTypeDateTime,
				Optional: true,
				Def: map[string]interface{}{
					"func": "CURRENT_TIMESTAMP",
				},
			},
		},
	}
	return meta.NewMetaFactory(nil).FactoryMeta(historyMetaDescription)
}

func (mm *MigrationManager) Run(migrationDescription *migrations_description.MigrationDescription, globalTransaction *transactions.GlobalTransaction, shouldRecord bool) (updatedMetaDescription *description.MetaDescription, err error) {
	migration, err := migrations.NewMigrationFactory(mm.metaStore, globalTransaction, mm.metaDescriptionSyncer).Factory(migrationDescription)
	if err != nil {
		return nil, err
	}

	for _, spawnedMigrationDescription := range migration.RunBefore {
		if _, err := mm.Run(spawnedMigrationDescription, globalTransaction, false); err != nil { //do not record applied spawned migrations, because of their ephemeral nature
			return nil, err
		}
	}

	if err := mm.canApplyMigration(migration, globalTransaction.DbTransaction); err != nil {
		return nil, err
	}

	var metaDescriptionToApply *description.MetaDescription

	//metaDescription should be retrieved again because it may mutate during runBefore migrations(eg automatically added outer link was removed)
	if migration.ApplyTo != nil {
		metaDescriptionToApply, _, err = mm.metaDescriptionSyncer.Get(migration.ApplyTo.Name)
		if err != nil {
			return nil, err
		}
	}

	for _, operation := range migration.Operations {
		//metaToApply should mutate only within iterations, not inside iteration
		updatedMetaDescription, err = operation.SyncMetaDescription(metaDescriptionToApply, globalTransaction.MetaDescriptionTransaction, mm.metaDescriptionSyncer)
		if err != nil {
			return nil, err
		} else {
			err := operation.SyncDbDescription(metaDescriptionToApply, globalTransaction.DbTransaction, mm.metaDescriptionSyncer)
			if err != nil {
				return nil, err
			}
		}
		//mutate metaToApply
		metaDescriptionToApply = updatedMetaDescription
	}

	for _, spawnedMigrationDescription := range migration.RunAfter {
		if _, err := mm.Run(spawnedMigrationDescription, globalTransaction, false); err != nil { //do not record applied spawned migrations, because of their ephemeral nature
			return nil, err
		}
	}

	if shouldRecord {
		_, err = mm.recordAppliedMigration(migration, globalTransaction.DbTransaction)
		if err != nil {
			return nil, err
		}
	}

	return updatedMetaDescription, nil
}

func (mm *MigrationManager) DropHistory(transaction transactions.DbTransaction) error {
	historyMeta, err := mm.ensureHistoryTableExists(transaction)
	if err != nil {
		return err
	}
	return object.NewDeleteObjectOperation().SyncDbDescription(historyMeta.MetaDescription, transaction, mm.metaDescriptionSyncer)
}

func NewMigrationManager(metaStore *meta.MetaStore, manager *pg.DataManager, syncer meta.MetaDescriptionSyncer) *MigrationManager {
	return &MigrationManager{metaStore: metaStore, dataManager: manager, metaDescriptionSyncer: syncer}
}
