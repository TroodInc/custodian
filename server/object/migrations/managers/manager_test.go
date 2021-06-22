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
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)

	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)

	It("Creates migration history table if it does not exists", func() {
		dbTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		_, err = NewMigrationManager(
			metaStore, dbTransactionManager,
		).ensureHistoryTableExists()
		Expect(err).To(BeNil())

		metaDdl, err := object.MetaDDLFromDB(dbTransaction.Transaction().(*sql.Tx), historyMetaName)

		Expect(err).To(BeNil())

		Expect(metaDdl.Table).To(Equal(object.GetTableName(historyMetaName)))
		Expect(metaDdl.Columns).To(HaveLen(7))

		dbTransactionManager.RollbackTransaction(dbTransaction)
	})

	It("Records migration", func() {
		migrationUid := utils.RandomString(8)
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		migrationHistoryId, err := NewMigrationManager(
			metaStore, dbTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).To(BeNil())

		Expect(migrationHistoryId).To(Equal(migrationUid))
	})

	It("Does not apply same migration twice", func() {
		migrationUid := utils.RandomString(8)
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		_, err := NewMigrationManager(
			metaStore, dbTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).To(BeNil())

		_, err = NewMigrationManager(
			metaStore, dbTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).NotTo(BeNil())
	})
})
