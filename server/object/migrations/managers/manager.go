package managers

import (
	"custodian/server/auth"
	"custodian/server/errors"
	_migrations "custodian/server/migrations"
	migrations_description "custodian/server/migrations/description"
	"custodian/server/migrations/migrations"
	object2 "custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/migrations/operations/object"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

const historyMetaName = "__custodian_objects_migration_history__"

type MigrationManager struct {
	metaSyncer                *object2.PgMetaDescriptionSyncer
	migrationSyncer           *object2.DbMetaDescriptionSyncer
	processor                *object2.Processor
	globalTransactionManager *object2.PgDbTransactionManager
}

func (mm *MigrationManager) Get(name string) (*object2.Record, error) {
	historyMeta, _ := mm.ensureHistoryTableExists()
	return mm.processor.Get(historyMeta.Name, name, nil, nil, 1, true)
}

func (mm *MigrationManager) List(filter string) (int, []*object2.Record, error) {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return 0, nil, err
	}
	return mm.processor.GetBulk(historyMeta.Name, filter, nil, nil, 1, true)
}

func (mm *MigrationManager) Apply(migrationDescription *migrations_description.MigrationDescription, shouldRecord bool, fake bool) (updatedMetaDescription *description.MetaDescription, err error) {
	if migration, err := migrations.NewMigrationFactory(mm.metaSyncer).FactoryForward(migrationDescription); err == nil {
		if err := mm.canApplyMigration(migration); err != nil {
			return nil, err
		}

		result, err := mm.runMigration(migration, shouldRecord, fake)
		if err != nil {
			return nil, err
		}
		return result, err
	}

	return nil, err

}

//Rollback object to the given migration`s state
func (mm *MigrationManager) RollBackTo(migrationId string, shouldRecord bool, fake bool) (*description.MetaDescription, error) {
	subsequentMigrations, err := mm.getSubsequentMigrations(migrationId)
	if err != nil {
		return nil, err
	}

	var updatedMetaDescription *description.MetaDescription
	for _, subsequentMigration := range subsequentMigrations {
		if err != nil {
			return nil, err
		}

		if !fake {
			updatedMetaDescription, err = mm.rollback(subsequentMigration, shouldRecord)
			if err != nil {
				return nil, err
			}
		} else {
			migration, err := migrations.NewMigrationFactory(mm.metaSyncer).FactoryBackward(subsequentMigration)
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

func (mm *MigrationManager) rollback(migrationDescription *migrations_description.MigrationDescription, shouldRecord bool) (updatedMetaDescription *description.MetaDescription, err error) {
	// Get a state which an object was in
	var previousMetaDescriptionState *description.MetaDescription
	if len(migrationDescription.DependsOn) > 0 {
		parentMigrationRecord, err := mm.Get(migrationDescription.DependsOn[0])
		if err != nil {
			return nil, err
		}
		parentMigrationDescription := migrations_description.MigrationDescriptionFromRecord(parentMigrationRecord)
		previousMetaDescriptionState = parentMigrationDescription.MetaDescription
	}

	// revert migrationDescription
	migrationDescription, err = migrations_description.NewReversionMigrationDescriptionService().Revert(previousMetaDescriptionState, migrationDescription)
	if err != nil {
		return nil, err
	}

	// and run it
	migration, err := migrations.NewMigrationFactory(mm.metaSyncer).FactoryBackward(migrationDescription)
	if err != nil {
		return nil, err
	}
	return mm.runMigration(migration, shouldRecord, false)
}

func (mm *MigrationManager) runMigration(migration *migrations.Migration, shouldRecord bool, fake bool) (updatedMetaDescription *description.MetaDescription, err error) {
	for _, spawnedMigrationDescription := range migration.RunBefore {
		if _, err := mm.Apply(spawnedMigrationDescription, false, fake); err != nil { //do not record applied spawned migrations, because of their ephemeral nature
			return nil, err
		}
	}

	var metaDescriptionToApply *description.MetaDescription

	// metaDescription should be retrieved again because it may mutate during runBefore migrations(eg automatically added outer link was removed)
	if migration.ApplyTo != nil {
		metaDescriptionToApply, _, err = mm.metaSyncer.Get(migration.ApplyTo.Name)
		if err != nil {
			return nil, err
		}
	}

	globalTransaction, err := mm.globalTransactionManager.BeginTransaction()
	if err != nil {
		return nil, err
	}
	for _, operation := range migration.Operations {
		//metaToApply should mutate only within iterations, not inside iteration
		if !fake {
			updatedMetaDescription, err = operation.SyncMetaDescription(metaDescriptionToApply, mm.metaSyncer)
			if err != nil {
				return nil, err
			} else {
				err := operation.SyncDbDescription(metaDescriptionToApply, globalTransaction, mm.metaSyncer)
				if err != nil {
					mm.globalTransactionManager.RollbackTransaction(globalTransaction)
					return nil, err
				}
			}
			metaDescriptionToApply = updatedMetaDescription
		}
		//mutate metaToApply
	}
	mm.globalTransactionManager.CommitTransaction(globalTransaction)

	for _, spawnedMigrationDescription := range migration.RunAfter {
		if _, err := mm.Apply(spawnedMigrationDescription, false, fake); err != nil { //do not record applied spawned migrations, because of their ephemeral nature
			return nil, err
		}
	}

	//assign MetaDescription to a migrationDescription, therefore it could be stored to a file
	migration.MigrationDescription.MetaDescription = updatedMetaDescription.Clone()
	if shouldRecord {
		if migration.IsForward() {
			_, err = mm.recordAppliedMigration(migration)
		} else {
			err = mm.removeAppliedMigration(migration)
		}
	}

	return updatedMetaDescription, err
}

func (mm *MigrationManager) DropHistory() error {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return err
	}

	transaction, err := mm.globalTransactionManager.BeginTransaction()
	if err != nil {
		return err
	}
	err = object.NewDeleteObjectOperation().SyncDbDescription(historyMeta.MetaDescription, transaction, mm.migrationSyncer)
	if err != nil {
		mm.globalTransactionManager.RollbackTransaction(transaction)
		return err
	}

	mm.globalTransactionManager.CommitTransaction(transaction)
	return nil
}

//return a list of preceding migrations for the given object
//*preceding migrations have the same predecessor*
func (mm *MigrationManager) GetPrecedingMigrationsForObject(objectName string) ([]*object2.Record, error) {
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

	rqlFilter := "eq(applyTo," + objectName + ")"
	if latestMigration.Data["dependsOn"] != "" {
		rqlFilter = rqlFilter + ",eq(dependsOn," + latestMigration.Data["dependsOn"].(string) + ")"
	}

	_, latestMigrations, err := mm.processor.GetBulk(historyMeta.Name, rqlFilter, nil, nil, 1, true)

	return latestMigrations, err
}

//return a latest applied migration for the given object
func (mm *MigrationManager) getLatestMigrationForObject(objectName string) (*object2.Record, error) {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return nil, err
	}

	rqlFilter := "eq(applyTo," + objectName + "),sort(-order),limit(0,1)"

	_, lastMigrationData, err := mm.processor.GetBulk(historyMeta.Name, rqlFilter, nil, nil, 1, true)
	if err != nil {
		return nil, err
	}

	if len(lastMigrationData) > 0 {
		return lastMigrationData[0], nil
	}

	return nil, nil
}

//return a list of migrations which were applied after the given one
func (mm *MigrationManager) getSubsequentMigrations(migrationId string) ([]*migrations_description.MigrationDescription, error) {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return nil, err
	}

	migration, err := mm.processor.Get(historyMeta.Name, migrationId, nil, nil, 1, false)
	if err != nil {
		return nil, err
	}

	var subsequentMigrations []*migrations_description.MigrationDescription
	if migration != nil {
		rqlFilter := "gt(order," + url.QueryEscape(strconv.Itoa(int(migration.Data["order"].(float64)))) + "),sort(-order)"

		_, migrationRecords, _ := mm.processor.GetBulk(historyMeta.Name, rqlFilter, nil, nil, 1, true)

		for _, mr := range migrationRecords {
			subsequentMigrations = append(subsequentMigrations, migrations_description.MigrationDescriptionFromRecord(mr))
		}
	}
	return subsequentMigrations, err
}

func (mm *MigrationManager) recordAppliedMigration(migration *migrations.Migration) (string, error) {
	historyMeta, err := mm.ensureHistoryTableExists()
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

	operations, _ := json.Marshal(migration.MigrationDescription.Operations)
	meta_state, _ := json.Marshal(migration.MigrationDescription.MetaDescription)

	migrationData := map[string]interface{}{
		"created":    time.Now().UTC().Format("2006-01-02T15:04:05.123456789Z07:00"),
		"id":         migration.Id,
		"dependsOn":  predecessorId,
		"applyTo":    metaName,
		"operations": string(operations),
		"meta_state": string(meta_state),
	}

	migrationRecord, err := mm.processor.CreateRecord(historyMeta.Name, migrationData, auth.User{})

	if err != nil {
		return "", err
	}

	return migrationRecord.PkAsString(), nil
}

func (mm *MigrationManager) removeAppliedMigration(migration *migrations.Migration) error {
	historyMeta, err := mm.ensureHistoryTableExists()
	if err != nil {
		return err
	}

	_, err = mm.processor.RemoveRecord(historyMeta.Name, migration.Id, auth.User{})
	if err != nil {
		return err
	}

	return nil
}

func (mm *MigrationManager) ensureHistoryTableExists() (*object2.Meta, error) {
	transaction, err := mm.globalTransactionManager.BeginTransaction()
	if err != nil {
		return nil, err
	}
	_, err = object2.MetaDDLFromDB(transaction.Transaction(), historyMetaName)
	doesNotExist := false
	if err != nil {
		switch castError := err.(type) {
		case *object2.DDLError:
			if castError.Code() == object2.ErrNotFound {
				doesNotExist = true
			} else {
				mm.globalTransactionManager.RollbackTransaction(transaction)
				return nil, err
			}
		default:
			mm.globalTransactionManager.RollbackTransaction(transaction)
			return nil, err
		}
	}

	historyMeta, err := mm.factoryHistoryMeta()
	if err != nil {
		mm.globalTransactionManager.RollbackTransaction(transaction)
		return nil, err
	}

	if doesNotExist {
		if err = object.NewCreateObjectOperation(historyMeta.MetaDescription).SyncDbDescription(nil, transaction, mm.migrationSyncer); err != nil {
			mm.globalTransactionManager.RollbackTransaction(transaction)
			return nil, err
		}
	}
	mm.globalTransactionManager.CommitTransaction(transaction)
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
	if err != nil {
		return err
	}
	if result != nil {
		return errors.NewValidationError(
			_migrations.MigrationErrorAlreadyHasBeenApplied,
			fmt.Sprintf("Migration with ID '%s' has already been applied", migration.Id),
			nil,
		)
	}
	return nil
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
	}

	return nil
}

func (mm *MigrationManager) factoryHistoryMeta() (*object2.Meta, error) {
	historyMetaDescription := &description.MetaDescription{
		Name: historyMetaName,
		Key:  "id",
		Fields: []description.Field{
			{
				Name:     "applyTo",
				Type:     description.FieldTypeString,
				Optional: false,
			}, {
				Name:     "id",
				Type:     description.FieldTypeString,
				Optional: false,
			}, {
				Name:     "dependsOn",
				Type:     description.FieldTypeString,
				Optional: false,
			}, {
				Name:        "created",
				Type:        description.FieldTypeDateTime,
				NowOnCreate: true,
			}, {
				Name: "order",
				Type: description.FieldTypeNumber,
				Def: map[string]interface{}{
					"func": "nextval",
				},
			}, {
				Name:     "operations",
				Type:     description.FieldTypeString,
				Optional: false,
			}, {
				Name:     "meta_state",
				Type:     description.FieldTypeString,
				Optional: false,
			},
		},
	}
	return object2.NewMetaFactory(nil).FactoryMeta(historyMetaDescription)
}

func NewMigrationManager(metaSyncer *object2.PgMetaDescriptionSyncer, gtm *object2.PgDbTransactionManager) *MigrationManager {
	migrationSyncer := object2.NewDbMetaDescriptionSyncer(gtm)
	migrationStore := object2.NewStore(migrationSyncer, gtm)
	processor, _ := object2.NewProcessor(migrationStore, gtm)

	return &MigrationManager{metaSyncer, migrationSyncer, processor, gtm}
}
