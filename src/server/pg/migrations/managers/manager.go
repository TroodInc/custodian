package managers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"server/data"
	"server/data/record"
	"server/errors"
	_migrations "server/migrations"
	migrations_description "server/migrations/description"
	"server/migrations/migrations"
	"server/object/description"
	"server/object/meta"
	"server/pg"
	"server/pg/migrations/operations/object"
	"server/transactions"
	"net/url"
	"strconv"
)

const historyMetaName = "__custodian_objects_migration_history__"

type MigrationManager struct {
	metaStore             *meta.MetaStore
	migrationStore        *meta.MetaStore
	dataManager           *pg.DataManager
	globalTransactionManager *transactions.GlobalTransactionManager
}

func (mm *MigrationManager) Get(name string) (*record.Record, error) {
	processor, err := data.NewProcessor(mm.migrationStore, mm.dataManager, mm.globalTransactionManager.DbTransactionManager)
	if err != nil {
		return nil, err
	}

	historyMeta, err := mm.ensureHistoryTableExists()

	return processor.Get(historyMeta.Name, name, nil, nil, 1, true)
}

func (mm *MigrationManager) List(filter string) ([]*record.Record, error) {
	processor, err := data.NewProcessor(mm.migrationStore, mm.dataManager, mm.globalTransactionManager.DbTransactionManager)
	historyMeta, err := mm.ensureHistoryTableExists()
	_, appliedMigrations, err := processor.GetBulk(historyMeta.Name, filter, nil, nil, 1, true)

	return appliedMigrations, err
}

func (mm *MigrationManager) Apply(migrationDescription *migrations_description.MigrationDescription, shouldRecord bool, fake bool) (updatedMetaDescription *description.MetaDescription, err error) {
	globalTransaction, _ := mm.globalTransactionManager.BeginTransaction(nil)
	migration, err := migrations.NewMigrationFactory(mm.metaStore.MetaDescriptionSyncer).FactoryForward(migrationDescription)
	if err != nil {
		mm.globalTransactionManager.RollbackTransaction(globalTransaction)
		return nil, err
	}

	if err := mm.canApplyMigration(migration, globalTransaction.DbTransaction); err != nil {
		mm.globalTransactionManager.RollbackTransaction(globalTransaction)
		return nil, err
	}

	result, err := mm.runMigration(migration, globalTransaction, shouldRecord, fake)
	if err != nil {
		mm.globalTransactionManager.RollbackTransaction(globalTransaction)
		return nil, err
	}

	mm.globalTransactionManager.CommitTransaction(globalTransaction)

	return result, err
}

func (mm *MigrationManager) FakeApply(migrationDescription *migrations_description.MigrationDescription, globalTransaction *transactions.GlobalTransaction) (err error) {
	migration, err := migrations.NewMigrationFactory(mm.metaStore.MetaDescriptionSyncer).FactoryForward(migrationDescription)
	if err != nil {
		return err
	}

	_, err = mm.recordAppliedMigration(migration, globalTransaction.DbTransaction)
	if err != nil {
		return err
	}

	return nil
}

//Rollback object to the given migration`s state
func (mm *MigrationManager) RollBackTo(migrationId string, globalTransaction *transactions.GlobalTransaction, shouldRecord bool, fake bool) (*description.MetaDescription, error) {
	subsequentMigrations, err := mm.getSubsequentMigrations(migrationId, globalTransaction.DbTransaction)
	if err != nil {
		return nil, err
	}

	var updatedMetaDescription *description.MetaDescription
	for _, subsequentMigration := range subsequentMigrations {
		if err != nil {
			return nil, err
		}

		if !fake {
			updatedMetaDescription, err = mm.rollback(subsequentMigration, globalTransaction, shouldRecord)
			if err != nil {
				return nil, err
			}
		} else {
			migration, err := migrations.NewMigrationFactory(mm.metaStore.MetaDescriptionSyncer).FactoryBackward(subsequentMigration)
			if err != nil {
				return nil, err
			}

			err = mm.removeAppliedMigration(migration, globalTransaction.DbTransaction)
			if err != nil {
				return nil, err
			}
		}
	}
	return updatedMetaDescription, nil
}

func (mm *MigrationManager) rollback(migrationDescription *migrations_description.MigrationDescription, globalTransaction *transactions.GlobalTransaction, shouldRecord bool) (updatedMetaDescription *description.MetaDescription, err error) {
	//Get a state which an object was in
	var previousMetaDescriptionState *description.MetaDescription
	if len(migrationDescription.DependsOn) > 0 {
		parentMigrationRecord, err := mm.Get(migrationDescription.DependsOn[0])
		if err != nil {
			return nil, err
		}
		parentMigrationDescription := migrations_description.MigrationDescriptionFromRecord(parentMigrationRecord)
		previousMetaDescriptionState = parentMigrationDescription.MetaDescription
	}

	//revert migrationDescription
	migrationDescription, err = migrations_description.NewReversionMigrationDescriptionService().Revert(previousMetaDescriptionState, migrationDescription)
	if err != nil {
		return nil, err
	}

	//and run it
	migration, err := migrations.NewMigrationFactory(mm.metaStore.MetaDescriptionSyncer).FactoryBackward(migrationDescription)
	if err != nil {
		return nil, err
	}
	return mm.runMigration(migration, globalTransaction, shouldRecord, false)
}

func (mm *MigrationManager) runMigration(migration *migrations.Migration, globalTransaction *transactions.GlobalTransaction, shouldRecord bool, fake bool) (updatedMetaDescription *description.MetaDescription, err error) {
	for _, spawnedMigrationDescription := range migration.RunBefore {
		if _, err := mm.Apply(spawnedMigrationDescription,false, fake); err != nil { //do not record applied spawned migrations, because of their ephemeral nature
			return nil, err
		}
	}

	var metaDescriptionToApply *description.MetaDescription

	//metaDescription should be retrieved again because it may mutate during runBefore migrations(eg automatically added outer link was removed)
	if migration.ApplyTo != nil {
		metaDescriptionToApply, _, err = mm.metaStore.MetaDescriptionSyncer.Get(migration.ApplyTo.Name)
		if err != nil {
			return nil, err
		}
	}

	for _, operation := range migration.Operations {
		//metaToApply should mutate only within iterations, not inside iteration
		updatedMetaDescription, err = operation.SyncMetaDescription(metaDescriptionToApply, globalTransaction.MetaDescriptionTransaction, mm.metaStore.MetaDescriptionSyncer)
		if err != nil {
			return nil, err
		} else {
			err := operation.SyncDbDescription(metaDescriptionToApply, globalTransaction.DbTransaction, mm.metaStore.MetaDescriptionSyncer)
			if err != nil {
				return nil, err
			}
		}
		//mutate metaToApply
		metaDescriptionToApply = updatedMetaDescription
	}

	for _, spawnedMigrationDescription := range migration.RunAfter {
		if _, err := mm.Apply(spawnedMigrationDescription, false, fake); err != nil { //do not record applied spawned migrations, because of their ephemeral nature
			return nil, err
		}
	}

	//assign MetaDescription to a migrationDescription, therefore it could be stored to a file
	migration.MigrationDescription.MetaDescription = updatedMetaDescription.Clone()
	if shouldRecord {
		if migration.IsForward() {
			_, err = mm.recordAppliedMigration(migration, globalTransaction.DbTransaction)
		} else {
			err = mm.removeAppliedMigration(migration, globalTransaction.DbTransaction)
		}
	}

	return updatedMetaDescription, nil
}

func (mm *MigrationManager) DropHistory(transaction transactions.DbTransaction) error {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return err
	}
	return object.NewDeleteObjectOperation().SyncDbDescription(historyMeta.MetaDescription, transaction, mm.migrationStore.MetaDescriptionSyncer)
}

//return a list of preceding migrations for the given object
//*preceding migrations have the same predecessor*
func (mm *MigrationManager) GetPrecedingMigrationsForObject(objectName string, transaction transactions.DbTransaction) ([]record.Record, error) {
	historyMeta, err := mm.ensureHistoryTableExists()
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

	var latestMigrations []record.Record
	callbackFunction := func(obj *record.Record) error {
		latestMigrations = append(latestMigrations, *record.NewRecord(historyMeta, obj.Data))
		return nil
	}
	processor, err := data.NewProcessor(mm.metaStore, mm.dataManager, mm.globalTransactionManager.DbTransactionManager)
	if err != nil {
		return nil, err

	}
	_, err = processor.ShadowGetBulk(transaction, historyMeta, rqlFilter, 1, true, callbackFunction)

	return latestMigrations, err
}

//return a latest applied migration for the given object
func (mm *MigrationManager) getLatestMigrationForObject(objectName string, transaction transactions.DbTransaction) (*record.Record, error) {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return nil, err
	}

	processor, err := data.NewProcessor(mm.metaStore, mm.dataManager, mm.globalTransactionManager.DbTransactionManager)
	if err != nil {
		return nil, err

	}
	rqlFilter := "eq(object," + objectName + "),sort(-order),limit(0,1)"

	var lastMigrationData map[string]interface{}
	callbackFunction := func(obj *record.Record) error {
		lastMigrationData = obj.Data
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

//return a list of migrations which were applied after the given one
func (mm *MigrationManager) getSubsequentMigrations(migrationId string, transaction transactions.DbTransaction) ([]*migrations_description.MigrationDescription, error) {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return nil, err
	}

	processor, err := data.NewProcessor(mm.metaStore, mm.dataManager, mm.globalTransactionManager.DbTransactionManager)
	if err != nil {
		return nil, err

	}

	migration, err := processor.Get(historyMeta.Name, migrationId, nil, nil, 1, false)
	if err != nil {
		return nil, err
	}

	rqlFilter := "gt(order," + url.QueryEscape(strconv.Itoa(int(migration.Data["order"].(float64)))) + "),sort(-order)"

	var subsequentMigrations []*migrations_description.MigrationDescription
	_, migrationRecords, err := processor.GetBulk(historyMeta.Name, rqlFilter, nil, nil, 1, true)

	for _, mr := range migrationRecords {
		subsequentMigrations = append(subsequentMigrations, migrations_description.MigrationDescriptionFromRecord(mr))
	}

	return subsequentMigrations, err
}

func (mm *MigrationManager) recordAppliedMigration(migration *migrations.Migration, transaction transactions.DbTransaction) (string, error) {
	historyMeta, err := mm.ensureHistoryTableExists()
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

	operations, _ := json.Marshal(migration.Operations)
	meta_sate, _ := json.Marshal(migration.MigrationDescription.MetaDescription)

	migrationRecord := record.NewRecord(historyMeta, map[string]interface{}{
		"migration_id":   migration.Id,
		"predecessor_id": predecessorId,
		"object":         metaName,
		"operations":  operations,
		"meta_state": meta_sate,
	})
	migrationRecord.PrepareData(record.RecordOperationTypeCreate)
	operation, err := mm.dataManager.PrepareCreateOperation(historyMeta, []map[string]interface{}{migrationRecord.RawData})
	if err != nil {
		return "", err
	}

	if e := transaction.Execute([]transactions.Operation{operation}); e != nil {
		return "", e
	}

	return migrationRecord.PkAsString(), nil
}

func (mm *MigrationManager) removeAppliedMigration(migration *migrations.Migration, transaction transactions.DbTransaction) error {
	historyMeta, err := mm.ensureHistoryTableExists()
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

func (mm *MigrationManager) ensureHistoryTableExists() (*meta.Meta, error) {
	transaction, err := mm.globalTransactionManager.DbTransactionManager.BeginTransaction()
	_, err = pg.MetaDDLFromDB(transaction.Transaction().(*sql.Tx), historyMetaName)
	doesNotExist := false
	if err != nil {
		switch castError := err.(type) {
		case *pg.DDLError:
			if castError.Code() == pg.ErrNotFound {
				doesNotExist = true
			} else {
				mm.globalTransactionManager.DbTransactionManager.RollbackTransaction(transaction)
				return nil, err
			}
		default:
			mm.globalTransactionManager.DbTransactionManager.RollbackTransaction(transaction)
			return nil, err
		}
	}

	historyMeta, err := mm.factoryHistoryMeta()
	if err != nil {
		mm.globalTransactionManager.DbTransactionManager.RollbackTransaction(transaction)
		return nil, err
	}

	if doesNotExist {
		if err = object.NewCreateObjectOperation(historyMeta.MetaDescription).SyncDbDescription(nil, transaction, mm.migrationStore.MetaDescriptionSyncer); err != nil {
			mm.globalTransactionManager.DbTransactionManager.RollbackTransaction(transaction)
			return nil, err
		}
	}
	mm.globalTransactionManager.DbTransactionManager.CommitTransaction(transaction)
	return historyMeta, nil
}

func (mm *MigrationManager) canApplyMigration(migration *migrations.Migration, transaction transactions.DbTransaction) error {
	if err := mm.migrationIsNotAppliedYet(migration, transaction); err != nil {
		return err
	}
	for _, parentId := range migration.DependsOn {
		if err := mm.migrationParentIsValid(migration, parentId, transaction); err != nil {
			return err
		}
	}
	return nil
}

func (mm *MigrationManager) migrationIsNotAppliedYet(migration *migrations.Migration, transaction transactions.DbTransaction) error {
	historyMeta, err := mm.ensureHistoryTableExists()
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
		return errors.NewValidationError(
			_migrations.MigrationErrorAlreadyHasBeenApplied,
			fmt.Sprintf("Migration with ID '%s' has already been applied", migration.Id),
			nil,
		)
	}
}

func (mm *MigrationManager) migrationParentIsValid(migration *migrations.Migration, parentMigrationId string, transaction transactions.DbTransaction) error {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return err
	}

	filters := map[string]interface{}{"migration_id": parentMigrationId}
	fields := []*meta.FieldDescription{historyMeta.Key}
	result, err := mm.dataManager.GetAll(historyMeta, fields, filters, transaction)
	if err != nil {
		return nil
	}
	if len(result) == 0 {
		return errors.NewValidationError(
			_migrations.MigrationErrorInvalidDescription,
			fmt.Sprintf("Migration with ID '%s' has unknown parent '%s'", migration.Id, parentMigrationId),
			nil,
		)
	} else {
		return nil
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
			}, {
				Name:     "migration_id",
				Type:     description.FieldTypeString,
				Optional: false,
			}, {
				Name:     "predecessor_id",
				Type:     description.FieldTypeString,
				Optional: false,
			}, {
				Name:      "created",
				Type:        description.FieldTypeDateTime,
				NowOnCreate: true,
			}, {
				Name: "order",
				Type: description.FieldTypeNumber,
				Def: map[string]interface{}{
					"func": "nextval",
				},
			}, {
				Name: "operations",
				Type: description.FieldTypeString,
				Optional: false,
			}, {
				Name: "meta_state",
				Type: description.FieldTypeString,
				Optional: false,
			},
		},
	}
	return meta.NewMetaFactory(nil).FactoryMeta(historyMetaDescription)
}

func NewMigrationManager(metaStore *meta.MetaStore, migrationStore *meta.MetaStore, manager *pg.DataManager, gtm *transactions.GlobalTransactionManager) *MigrationManager {
	return &MigrationManager{metaStore, migrationStore, manager, gtm}
}
