package managers

import (
	"custodian/server/migrations/description"
	"custodian/server/migrations/migrations"
	"custodian/server/object"

	"custodian/utils"
	"database/sql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MigrationManager", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)

	metaStore := object.NewStore(metaDescriptionSyncer, syncer, dbTransactionManager)

	It("Creates migration history table if it does not exists", func() {
		dbTransaction, err := dbTransactionManager.BeginTransaction()
		if err != nil {
			dbTransactionManager.RollbackTransaction(dbTransaction)
			Expect(err).To(BeNil())
		}

		_, err = NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, dbTransactionManager,
		).ensureHistoryTableExists()
		if err != nil {
			dbTransactionManager.RollbackTransaction(dbTransaction)
			Expect(err).To(BeNil())
		}

		metaDdl, err := object.MetaDDLFromDB(dbTransaction.Transaction().(*sql.Tx), historyMetaName)

		if err != nil {
			dbTransactionManager.RollbackTransaction(dbTransaction)
			Expect(err).To(BeNil())
		}

		Expect(metaDdl.Table).To(Equal(object.GetTableName(historyMetaName)))
		Expect(metaDdl.Columns).To(HaveLen(7))

		dbTransactionManager.RollbackTransaction(dbTransaction)
	})

	It("Records migration", func() {
		migrationUid := utils.RandomString(8)
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		migrationHistoryId, err := NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, dbTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).To(BeNil())

		Expect(migrationHistoryId).To(Equal(migrationUid))
	})

	It("Does not apply same migration twice", func() {
		migrationUid := utils.RandomString(8)
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		_, err := NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, dbTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).To(BeNil())

		_, err = NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, dbTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).NotTo(BeNil())
	})
})
