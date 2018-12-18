package keepers

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

type MigrationHistoryKeeper struct {
	dataManager *pg.DataManager
}

func (mhk *MigrationHistoryKeeper) RecordAppliedMigration(migration *migrations.Migration, transaction transactions.DbTransaction) (string, error) {
	historyMeta, err := mhk.ensureHistoryTableExists(transaction)
	if err != nil {
		return "", err
	}

	err = mhk.ensureMigrationNotRecordedYet(historyMeta, migration, transaction)
	if err != nil {
		return "", nil
	}

	migrationData := map[string]interface{}{"migration_id": migration.Id}
	operation, err := mhk.dataManager.PrepareCreateOperation(historyMeta, []map[string]interface{}{migrationData})
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

func (mhk *MigrationHistoryKeeper) ensureHistoryTableExists(transaction transactions.DbTransaction) (*meta.Meta, error) {
	_, err := pg.MetaDDLFromDB(transaction.Transaction().(*sql.Tx), pg.GetTableName(historyMetaName))
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

	historyMeta, err := mhk.factoryHistoryMeta()
	if err != nil {
		return nil, err
	}

	if doesNotExist {
		if err = new(object.CreateObjectOperation).SyncDbDescription(historyMeta, transaction); err != nil {
			return nil, err
		}
	}
	return historyMeta, nil
}

func (mhk *MigrationHistoryKeeper) ensureMigrationNotRecordedYet(historyMeta *meta.Meta, migration *migrations.Migration, transaction transactions.DbTransaction) error {
	filters := map[string]interface{}{"migration_id": migration.Id}
	fields := []*meta.FieldDescription{historyMeta.Key}
	result, err := mhk.dataManager.GetAll(historyMeta, fields, filters, transaction)
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

func (mhk *MigrationHistoryKeeper) factoryHistoryMeta() (*meta.Meta, error) {
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

func NewMigrationHouseKeeper(manager *pg.DataManager) *MigrationHistoryKeeper {
	return &MigrationHistoryKeeper{dataManager: manager}
}
