package managers

import (
	. "github.com/onsi/ginkgo"
	"server/pg"
	"utils"
	pg_transactions "server/pg/transactions"
	. "github.com/onsi/gomega"
	"database/sql"
	"server/migrations/migrations"
	"server/migrations/description"
	"server/object/meta"
)

var _ = Describe("MigrationManager", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)

	dataManager, _ := syncer.NewDataManager()
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer)
	//transaction managers
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)

	It("Creates migration history table if it does not exists", func() {
		dbTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		_, err = NewMigrationManager(metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath).ensureHistoryTableExists(dbTransaction)
		Expect(err).To(BeNil())

		metaDdl, err := pg.MetaDDLFromDB(dbTransaction.Transaction().(*sql.Tx), historyMetaName)

		Expect(err).To(BeNil())

		Expect(metaDdl.Table).To(Equal(pg.GetTableName(historyMetaName)))
		Expect(metaDdl.Columns).To(HaveLen(5))

		dbTransactionManager.RollbackTransaction(dbTransaction)
	})

	It("Records migration", func() {
		dbTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())
		migrationUid := "c1be598d"
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		migrationHistoryId, err := NewMigrationManager(metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath).recordAppliedMigration(migration, dbTransaction)
		Expect(err).To(BeNil())

		Expect(migrationHistoryId).To(Equal(migrationUid))

		dbTransactionManager.RollbackTransaction(dbTransaction)
	})

	It("Does not apply same migration twice", func() {
		dbTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())
		migrationUid := "c1be598d"
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		_, err = NewMigrationManager(metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath).recordAppliedMigration(migration, dbTransaction)
		Expect(err).To(BeNil())

		_, err = NewMigrationManager(metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath).recordAppliedMigration(migration, dbTransaction)
		Expect(err).NotTo(BeNil())

		dbTransactionManager.RollbackTransaction(dbTransaction)
	})
})
