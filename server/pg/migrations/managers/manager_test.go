package managers

import (
	. "github.com/onsi/ginkgo"
	"custodian/server/pg"
	"custodian/server/transactions"
	"custodian/utils"
	. "github.com/onsi/gomega"
	"database/sql"
	"custodian/server/migrations/migrations"
	"custodian/server/migrations/description"
	"custodian/server/object/meta"
)

var _ = Describe("MigrationManager", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := pg.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg.NewPgMetaDescriptionSyncer(dbTransactionManager)

	globalTransactionManager := transactions.NewGlobalTransactionManager(nil, dbTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	It("Creates migration history table if it does not exists", func() {
		dbTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		_, err = NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
		).ensureHistoryTableExists()
		Expect(err).To(BeNil())

		metaDdl, err := pg.MetaDDLFromDB(dbTransaction.Transaction().(*sql.Tx), historyMetaName)

		Expect(err).To(BeNil())

		Expect(metaDdl.Table).To(Equal(pg.GetTableName(historyMetaName)))
		Expect(metaDdl.Columns).To(HaveLen(7))

		dbTransactionManager.RollbackTransaction(dbTransaction)
	})

	It("Records migration", func() {
		migrationUid := utils.RandomString(8)
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		migrationHistoryId, err := NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).To(BeNil())

		Expect(migrationHistoryId).To(Equal(migrationUid))
	})

	It("Does not apply same migration twice", func() {
		migrationUid := utils.RandomString(8)
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		_, err := NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).To(BeNil())

		_, err = NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).NotTo(BeNil())
	})
})
