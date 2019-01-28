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
	"server/data/record"
	"server/data"
)

const historyMetaName = "__custodian_objects_migration_history__"

type MigrationManager struct {
	metaStore             *meta.MetaStore
	dataManager           *pg.DataManager
	metaDescriptionSyncer meta.MetaDescriptionSyncer
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

//return a list of preceding migrations for the given object
//*preceding migrations have the same predecessor*
func (mm *MigrationManager) GetPrecedingMigrationsForObject(objectName string, transaction transactions.DbTransaction) ([]*record.Record, error) {
	historyMeta, err := mm.ensureHistoryTableExists(transaction)
	if err != nil {
		return nil, err
	}

	latestMigration, err := mm.getLatestMigrationForObject(objectName, transaction)
	if err != nil {
		return nil, err
	}
	if latestMigration == nil {
		return nil, nil
	}

	rqlFilter := "eq(object," + objectName + ")"
	if latestMigration.Data["predecessor_id"] != "" {
		rqlFilter = rqlFilter + ",eq(predecessor_id," + latestMigration.Data["predecessor_id"].(string) + ")"
	}

	var latestMigrations []*record.Record
	callbackFunction := func(obj map[string]interface{}) error {
		latestMigrations = append(latestMigrations, record.NewRecord(historyMeta, obj))
		return nil
	}
	processor, err := data.NewProcessor(mm.metaStore, mm.dataManager)
	if err != nil {
		return nil, err

	}
	_, err = processor.ShadowGetBulk(transaction, historyMeta, rqlFilter, 1, true, callbackFunction)

	return latestMigrations, err
}

//return a latest applied migration for the given object
func (mm *MigrationManager) getLatestMigrationForObject(objectName string, transaction transactions.DbTransaction) (*record.Record, error) {
	historyMeta, err := mm.ensureHistoryTableExists(transaction)
	if err != nil {
		return nil, err
	}

	processor, err := data.NewProcessor(mm.metaStore, mm.dataManager)
	if err != nil {
		return nil, err

	}
	rqlFilter := "eq(object," + objectName + "),sort(-created),limit(0,1)"

	var lastMigrationData map[string]interface{}
	callbackFunction := func(obj map[string]interface{}) error {
		lastMigrationData = obj
		return nil
	}
	_, err = processor.ShadowGetBulk(transaction, historyMeta, rqlFilter, 1, true, callbackFunction)
	if err != nil {
		return nil, err
	}
	if lastMigrationData != nil {
		return record.NewRecord(historyMeta, lastMigrationData), nil
	} else {
		return nil, nil
	}
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

	metaName, err := migration.MetaName()
	if err != nil {
		return "", err
	}
	var predecessorId string
	if latestMigration, err := mm.getLatestMigrationForObject(metaName, transaction); err != nil {
		return "", err
	} else {
		if latestMigration != nil {
			predecessorId = latestMigration.Data["migration_id"].(string)
		} else {
			predecessorId = ""
		}
	}

	migrationData := map[string]interface{}{
		"migration_id":   migration.Id,
		"predecessor_id": predecessorId,
		"object":         metaName,
	}
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
			_migrations.MigrationErrorAlreadyHasBeenApplied,
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
				Name:     "object",
				Type:     description.FieldTypeString,
				Optional: false,
			},
			{
				Name:     "migration_id",
				Type:     description.FieldTypeString,
				Optional: false,
			},
			{
				Name:     "predecessor_id",
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

func NewMigrationManager(metaStore *meta.MetaStore, manager *pg.DataManager, syncer meta.MetaDescriptionSyncer) *MigrationManager {
	return &MigrationManager{metaStore: metaStore, dataManager: manager, metaDescriptionSyncer: syncer}
}
