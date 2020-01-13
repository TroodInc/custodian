package managers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"server/auth"
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
	processor			  *data.Processor
	globalTransactionManager *transactions.GlobalTransactionManager
}

func (mm *MigrationManager) Get(name string) (*record.Record, error) {
	historyMeta, _ := mm.ensureHistoryTableExists()

	return mm.processor.Get(historyMeta.Name, name, nil, nil, 1, true)
}

func (mm *MigrationManager) List(filter string) ([]*record.Record, error) {
	historyMeta, err := mm.ensureHistoryTableExists()

	_, appliedMigrations, err := mm.processor.GetBulk(historyMeta.Name, filter, nil, nil, 1, true)
	return appliedMigrations, err
}

func (mm *MigrationManager) Apply(migrationDescription *migrations_description.MigrationDescription, shouldRecord bool, fake bool) (updatedMetaDescription *description.MetaDescription, err error) {
	globalTransaction, _ := mm.globalTransactionManager.BeginTransaction(nil)
	migration, err := migrations.NewMigrationFactory(mm.metaStore.MetaDescriptionSyncer).FactoryForward(migrationDescription)
	if err != nil {
		mm.globalTransactionManager.RollbackTransaction(globalTransaction)
		return nil, err
	}

	if err := mm.canApplyMigration(migration); err != nil {
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

//Rollback object to the given migration`s state
func (mm *MigrationManager) RollBackTo(migrationID string, globalTransaction *transactions.GlobalTransaction, shouldRecord bool, fake bool) (*description.MetaDescription, error) {
	subsequentMigrations, err := mm.getSubsequentMigrations(migrationID)
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

			err = mm.removeAppliedMigration(migration)
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
			err = mm.removeAppliedMigration(migration)
		}
	}

	return updatedMetaDescription, nil
}

func (mm *MigrationManager) DropHistory() error {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return err
	}

	transaction, err := mm.globalTransactionManager.DbTransactionManager.BeginTransaction()
	err = object.NewDeleteObjectOperation().SyncDbDescription(historyMeta.MetaDescription, transaction, mm.migrationStore.MetaDescriptionSyncer)
	if err != nil {
		mm.globalTransactionManager.DbTransactionManager.RollbackTransaction(transaction)
		return err
	}

	mm.globalTransactionManager.DbTransactionManager.CommitTransaction(transaction)
	return nil
}

//return a list of preceding migrations for the given object
//*preceding migrations have the same predecessor*
func (mm *MigrationManager) GetPrecedingMigrationsForObject(objectName string) ([]*record.Record, error) {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return nil, err
	}

	latestMigration, err := mm.getLatestMigrationForObject(objectName)
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

	_, latestMigrations, err := mm.processor.GetBulk(historyMeta.Name, rqlFilter, nil, nil, 1, true)

	return latestMigrations, err
}

//return a latest applied migration for the given object
func (mm *MigrationManager) getLatestMigrationForObject(objectName string) (*record.Record, error) {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return nil, err
	}

	rqlFilter := "eq(object," + objectName + "),sort(-order),limit(0,1)"

	_, lastMigrationData, err := mm.processor.GetBulk(historyMeta.Name, rqlFilter, nil, nil,1, true)

	if len(lastMigrationData) > 0 {
		return lastMigrationData[0], err
	}

	return nil, err
}

//return a list of migrations which were applied after the given one
func (mm *MigrationManager) getSubsequentMigrations(migrationID string) ([]*migrations_description.MigrationDescription, error) {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return nil, err
	}

	migration, err := mm.processor.Get(historyMeta.Name, migrationID, nil, nil, 1, false)
	if err != nil {
		return nil, err
	}

	rqlFilter := "gt(order," + url.QueryEscape(strconv.Itoa(int(migration.Data["order"].(float64)))) + "),sort(-order)"

	var subsequentMigrations []*migrations_description.MigrationDescription
	_, migrationRecords, err := mm.processor.GetBulk(historyMeta.Name, rqlFilter, nil, nil, 1, true)

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

	err = mm.canApplyMigration(migration)
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

func (mm *MigrationManager) removeAppliedMigration(migration *migrations.Migration) error {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return err
	}

	// @todo: figure out User
	_, err = mm.processor.RemoveRecord(historyMeta.Name, migration.Id, auth.User{})

	return err
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

func (mm *MigrationManager) canApplyMigration(migration *migrations.Migration) error {
	if err := mm.migrationIsNotAppliedYet(migration); err != nil {
		return err
	}
	for _, parentId := range migration.DependsOn {
		if err := mm.migrationParentIsValid(migration, parentId); err != nil {
			return err
		}
	}
	return nil
}

func (mm *MigrationManager) migrationIsNotAppliedYet(migration *migrations.Migration) error {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return err
	}

	result, err := mm.processor.Get(historyMeta.Name, migration.Id, nil, nil, 1, true)
	if result != nil {
		return errors.NewValidationError(
			_migrations.MigrationErrorAlreadyHasBeenApplied,
			fmt.Sprintf("Migration with ID '%s' has already been applied", migration.Id),
			nil,
		)
	}

	return err
}

func (mm *MigrationManager) migrationParentIsValid(migration *migrations.Migration, parentMigrationId string) error {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return err
	}

	result, err := mm.processor.Get(historyMeta.Name, parentMigrationId, nil, nil, 1, true)

	if err != nil {
		return err
	}

	if result == nil {
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
	processor, _ := data.NewProcessor(migrationStore, manager, gtm.DbTransactionManager)
	return &MigrationManager{
		metaStore,
		migrationStore,
		manager,
		processor,
		gtm}
}
