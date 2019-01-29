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
	"server/migrations/storage"
	"net/url"
)

const historyMetaName = "__custodian_objects_migration_history__"

type MigrationManager struct {
	metaStore             *meta.MetaStore
	dataManager           *pg.DataManager
	metaDescriptionSyncer meta.MetaDescriptionSyncer
	migrationStorage      *storage.MigrationStorage
}

func (mm *MigrationManager) GetDescription(dbTransaction transactions.DbTransaction, migrationId string) (*migrations_description.MigrationDescription, error) {
	migrationDescription, err := mm.migrationStorage.Get(migrationId)
	if err != nil {
		return nil, err
	}

	return migrationDescription, nil
}

func (mm *MigrationManager) List(dbTransaction transactions.DbTransaction, filter string) ([]*record.Record, error) {
	historyMeta, err := mm.ensureHistoryTableExists(dbTransaction)
	if err != nil {
		return nil, err
	}

	var appliedMigrations []*record.Record
	callbackFunction := func(obj map[string]interface{}) error {
		appliedMigrations = append(appliedMigrations, record.NewRecord(historyMeta, obj))
		return nil
	}

	processor, err := data.NewProcessor(mm.metaStore, mm.dataManager)
	if err != nil {
		return nil, err
	}

	_, err = processor.ShadowGetBulk(dbTransaction, historyMeta, filter, 1, true, callbackFunction)
	return appliedMigrations, err
}

//Rollback object to the given migration`s state
func (mm *MigrationManager) RollBackTo(migrationId string, globalTransaction *transactions.GlobalTransaction, shouldRecord bool) (*description.MetaDescription, error) {
	subsequentMigrations, err := mm.getSubsequentMigrations(migrationId, globalTransaction.DbTransaction)
	if err != nil {
		return nil, err
	}

	var updatedMetaDescription *description.MetaDescription
	for _, subsequentMigration := range subsequentMigrations {
		migrationDescription, err := mm.migrationStorage.Get(subsequentMigration.Data["migration_id"].(string))
		if err != nil {
			return nil, err
		}

		updatedMetaDescription, err = mm.rollback(migrationDescription, globalTransaction, shouldRecord)
		if err != nil {
			return nil, err
		}

	}
	return updatedMetaDescription, nil
}

func (mm *MigrationManager) Apply(migrationDescription *migrations_description.MigrationDescription, globalTransaction *transactions.GlobalTransaction, shouldRecord bool) (updatedMetaDescription *description.MetaDescription, err error) {
	migration, err := migrations.NewMigrationFactory(mm.metaDescriptionSyncer).FactoryForward(migrationDescription)
	if err != nil {
		return nil, err
	}
	if err := mm.canApplyMigration(migration, globalTransaction.DbTransaction); err != nil {
		return nil, err
	}

	return mm.runMigration(migration, globalTransaction, shouldRecord)
}

func (mm *MigrationManager) rollback(migrationDescription *migrations_description.MigrationDescription, globalTransaction *transactions.GlobalTransaction, shouldRecord bool) (updatedMetaDescription *description.MetaDescription, err error) {
	//Get a state which an object was in
	var previousMetaDescriptionState *description.MetaDescription
	if len(migrationDescription.DependsOn) > 0 {
		parentMigrationDescription, err := mm.migrationStorage.Get(migrationDescription.DependsOn[0])
		if err != nil {
			return nil, err
		}
		previousMetaDescriptionState = parentMigrationDescription.MetaDescription
	}

	//revert migrationDescription
	migrationDescription, err = migrations_description.NewReversionMigrationDescriptionService().Revert(previousMetaDescriptionState, migrationDescription)
	if err != nil {
		return nil, err
	}

	//and run it
	migration, err := migrations.NewMigrationFactory(mm.metaDescriptionSyncer).FactoryBackward(migrationDescription)
	if err != nil {
		return nil, err
	}
	return mm.runMigration(migration, globalTransaction, shouldRecord)
}

func (mm *MigrationManager) runMigration(migration *migrations.Migration, globalTransaction *transactions.GlobalTransaction, shouldRecord bool) (updatedMetaDescription *description.MetaDescription, err error) {
	for _, spawnedMigrationDescription := range migration.RunBefore {
		if _, err := mm.Apply(spawnedMigrationDescription, globalTransaction, false); err != nil { //do not record applied spawned migrations, because of their ephemeral nature
			return nil, err
		}
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
		if _, err := mm.Apply(spawnedMigrationDescription, globalTransaction, false); err != nil { //do not record applied spawned migrations, because of their ephemeral nature
			return nil, err
		}
	}

	//assign MetaDescription to a migrationDescription, therefore it could be stored to a file
	migration.MigrationDescription.MetaDescription = updatedMetaDescription.Clone()
	if shouldRecord {
		if migration.IsForward() {
			migrationFileName, err := mm.migrationStorage.Store(&migration.MigrationDescription)
			if err != nil {
				return nil, err
			}

			_, err = mm.recordAppliedMigration(migration, globalTransaction.DbTransaction)
			if err != nil {
				if removeErr := mm.migrationStorage.RemoveFile(migrationFileName); removeErr != nil {
					return nil, _migrations.NewMigrationError(_migrations.MigrationErrorWhileWritingMigrationFile, err.Error()+"\r\n"+removeErr.Error())
				}
				return nil, err
			}
		} else {
			err := mm.migrationStorage.Remove(&migration.MigrationDescription)
			if err != nil {
				return nil, err
			}

			err = mm.removeAppliedMigration(migration, globalTransaction.DbTransaction)
			if err != nil {
				if removeErr := mm.migrationStorage.Remove(&migration.MigrationDescription); removeErr != nil {
					return nil, _migrations.NewMigrationError(_migrations.MigrationErrorWhileWritingMigrationFile, err.Error()+"\r\n"+removeErr.Error())
				}
				return nil, err
			}
		}
	}

	return updatedMetaDescription, nil
}

func (mm *MigrationManager) DropHistory(transaction transactions.DbTransaction) error {
	historyMeta, err := mm.ensureHistoryTableExists(transaction)
	if err != nil {
		return err
	}
	if err := mm.migrationStorage.Flush(); err != nil {
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

//return a list of migrations which were applied after the given one(for the same object)
func (mm *MigrationManager) getSubsequentMigrations(migrationId string, transaction transactions.DbTransaction) ([]*record.Record, error) {
	historyMeta, err := mm.ensureHistoryTableExists(transaction)
	if err != nil {
		return nil, err
	}

	processor, err := data.NewProcessor(mm.metaStore, mm.dataManager)
	if err != nil {
		return nil, err

	}

	migration, err := processor.ShadowGet(transaction, historyMeta, migrationId, 1, false)
	if err != nil {
		return nil, err
	}

	rqlFilter := "eq(object," + migration.Data["object"].(string) + "),gt(created," + url.QueryEscape(migration.Data["created"].(string)) + "),sort(-created)"

	var subsequentMigrations []*record.Record
	callbackFunction := func(obj map[string]interface{}) error {
		subsequentMigrations = append(subsequentMigrations, record.NewRecord(historyMeta, obj))
		return nil
	}
	_, err = processor.ShadowGetBulk(transaction, historyMeta, rqlFilter, 1, true, callbackFunction)
	return subsequentMigrations, err
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
	if len(migration.DependsOn) > 0 {
		predecessorId = migration.DependsOn[0]
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

	if e := transaction.Execute([]transactions.Operation{operation}); e != nil {
		return "", e
	}

	migrationIdStrValue, _ := historyMeta.Key.ValueAsString(migrationData[historyMeta.Key.Name])
	return migrationIdStrValue, nil
}

func (mm *MigrationManager) removeAppliedMigration(migration *migrations.Migration, transaction transactions.DbTransaction) error {
	historyMeta, err := mm.ensureHistoryTableExists(transaction)
	if err != nil {
		return err
	}
	fields := []*meta.FieldDescription{historyMeta.FindField("migration_id")}
	recordData, err := mm.dataManager.Get(historyMeta, fields, "migration_id", migration.Id, transaction)
	if err != nil {
		return nil
	}
	operation, err := mm.dataManager.PrepareRemoveOperation(record.NewRecord(historyMeta, recordData))
	if err != nil {
		return err
	}

	if err := transaction.Execute([]transactions.Operation{operation}); err != nil {
		return err
	}

	return nil
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
		Key:  "migration_id",
		Fields: []description.Field{
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

func NewMigrationManager(metaStore *meta.MetaStore, manager *pg.DataManager, syncer meta.MetaDescriptionSyncer, migrationStoragePath string) *MigrationManager {
	return &MigrationManager{metaStore: metaStore, dataManager: manager, metaDescriptionSyncer: syncer, migrationStorage: storage.NewMigrationStorage(migrationStoragePath)}
}
