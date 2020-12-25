package managers

import (
	. "github.com/onsi/ginkgo"
	"custodian/server/pg"
	"custodian/server/transactions"
	"custodian/utils"
	pg_transactions "custodian/server/pg/transactions"
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
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")
	//transaction managers
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(nil, dbTransactionManager)

	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)

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
