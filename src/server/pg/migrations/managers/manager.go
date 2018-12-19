package managers

import (
	"server/transactions"
	"server/pg"
	"database/sql"
	"server/object/meta"
	"server/object/description"
	"server/pg/migrations/operations/object"
	"server/migrations/migrations"
	pg_migrations "server/pg/migrations"
	"fmt"
)

const historyMetaName = "__custodian_objects_migration_history__"

type MigrationManager struct {
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
		if err = object.NewCreateObjectOperation(historyMeta).SyncDbDescription(nil, transaction); err != nil {
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
		return pg_migrations.NewPgMigrationError(
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

func (mm *MigrationManager) Run(migration *migrations.Migration, globalTransaction *transactions.GlobalTransaction) (updatedMeta *meta.Meta, err error) {
	if err := mm.canApplyMigration(migration, globalTransaction.DbTransaction); err != nil {
		return nil, err
	}
	metaToApply := migration.ApplyTo
	for _, operation := range migration.Operations {
		//metaToApply should mutate only within iterations, not inside iteration
		updatedMeta, err = operation.SyncMetaDescription(metaToApply, globalTransaction.MetaDescriptionTransaction, mm.metaDescriptionSyncer)
		if err != nil {
			return nil, err
		} else {
			err := operation.SyncDbDescription(metaToApply, globalTransaction.DbTransaction)
			if err != nil {
				return nil, err
			}
		}
		//mutate metaToApply
		metaToApply = updatedMeta
	}

	_, err = mm.recordAppliedMigration(migration, globalTransaction.DbTransaction)

	return updatedMeta, nil
}

func (mm *MigrationManager) DropHistory(transaction transactions.DbTransaction) error {
	historyMeta, err := mm.ensureHistoryTableExists(transaction)
	if err != nil {
		return err
	}
	return object.NewDeleteObjectOperation().SyncDbDescription(historyMeta, transaction)
}

func NewMigrationManager(manager *pg.DataManager, syncer meta.MetaDescriptionSyncer) *MigrationManager {
	return &MigrationManager{dataManager: manager, metaDescriptionSyncer: syncer}
}
