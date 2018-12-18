package keepers

import (
	. "github.com/onsi/ginkgo"
	"server/pg"
	"utils"
	pg_transactions "server/pg/transactions"
	. "github.com/onsi/gomega"
	"database/sql"
	"server/migrations/migrations"
	"server/migrations/description"
)

var _ = FDescribe("Migrations's keeper", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)

	It("Creates migration history table if it does not exists", func() {
		dbTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		_, err = NewMigrationHouseKeeper(dataManager).ensureHistoryTableExists(dbTransaction)
		Expect(err).To(BeNil())

		metaDdl, err := pg.MetaDDLFromDB(dbTransaction.Transaction().(*sql.Tx), historyMetaName)

		Expect(err).To(BeNil())

		Expect(metaDdl.Table).To(Equal(pg.GetTableName(historyMetaName)))
		Expect(metaDdl.Columns).To(HaveLen(3))

		dbTransactionManager.RollbackTransaction(dbTransaction)
	})

	It("Records migration", func() {
		dbTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())
		migrationUid := "c1be598d"
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{Id: migrationUid}}

		migrationHistoryId, err := NewMigrationHouseKeeper(dataManager).RecordAppliedMigration(migration, dbTransaction)
		Expect(err).To(BeNil())

		Expect(migrationHistoryId).To(Equal("1"))

		dbTransactionManager.RollbackTransaction(dbTransaction)
	})

	It("Does not apply same migration twice", func() {
		dbTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())
		migrationUid := "c1be598d"
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{Id: migrationUid}}

		_, err = NewMigrationHouseKeeper(dataManager).RecordAppliedMigration(migration, dbTransaction)
		Expect(err).To(BeNil())

		_, err = NewMigrationHouseKeeper(dataManager).RecordAppliedMigration(migration, dbTransaction)
		Expect(err).NotTo(BeNil())
		
		dbTransactionManager.RollbackTransaction(dbTransaction)
	})
})
