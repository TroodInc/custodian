package managers

import (
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/migrations/description"
	"server/migrations/migrations"
	"server/object/meta"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("MigrationManager", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()

	//transaction managers
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(nil, dbTransactionManager)

	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)
	migrationDBDescriptionSyncer := pg.NewDbMetaDescriptionSyncer(dbTransactionManager)
	migrationStore := meta.NewStore(migrationDBDescriptionSyncer, syncer, globalTransactionManager)
	migrationManager := NewMigrationManager(
		metaStore, migrationStore, dataManager, globalTransactionManager,
	)

	It("Creates migration history table if it does not exists", func() {
		_, err := migrationManager.ensureHistoryTableExists()
		Expect(err).To(BeNil())

		dbTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())
		metaDdl, err := pg.MetaDDLFromDB(dbTransaction.Transaction().(*sql.Tx), historyMetaName)
		dbTransactionManager.RollbackTransaction(dbTransaction)

		Expect(err).To(BeNil())

		Expect(metaDdl.Table).To(Equal(pg.GetTableName(historyMetaName)))
		Expect(metaDdl.Columns).To(HaveLen(7))
	})

	It("Records migration", func() {
		migrationUid := "c1be598d"
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		migrationHistoryId, err := migrationManager.recordAppliedMigration(migration)
		Expect(err).To(BeNil())

		Expect(migrationHistoryId).To(Equal(migrationUid))
	})

	It("Does not apply same migration twice", func() {
		migrationUid := "c1be598d"
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		_, err := migrationManager.recordAppliedMigration(migration)
		Expect(err).To(BeNil())

		_, err = migrationManager.recordAppliedMigration(migration)
		Expect(err).NotTo(BeNil())
	})
})
