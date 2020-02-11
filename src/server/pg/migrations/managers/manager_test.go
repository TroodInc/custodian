package managers

import (
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/migrations/description"
	"server/migrations/migrations"
	"server/object"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("MigrationManager", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")
	//transaction managers
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(nil, dbTransactionManager)

	metaStore := object.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

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
		migrationUid := "c1be598d"
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		migrationHistoryId, err := NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).To(BeNil())

		Expect(migrationHistoryId).To(Equal(migrationUid))
	})

	It("Does not apply same migration twice", func() {
		migrationUid := "c1be59xd"
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
