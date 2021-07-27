package managers

import (
	"custodian/server/migrations/description"
	"custodian/server/migrations/migrations"
	"custodian/server/object"

	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MigrationManager", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache())

	It("Creates migration history table if it does not exists", func() {

		NewMigrationManager(metaDescriptionSyncer, dbTransactionManager)
		dbTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		metaDdl, err := object.MetaDDLFromDB(dbTransaction.Transaction(), historyMetaName)

		Expect(err).To(BeNil())

		Expect(metaDdl.Table).To(Equal(object.GetTableName(historyMetaName)))
		Expect(metaDdl.Columns).To(HaveLen(8))

		dbTransaction.Rollback()
	})

	It("Records migration", func() {
		migrationUid := utils.RandomString(8)
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		migrationHistoryId, err := NewMigrationManager(
			metaDescriptionSyncer, dbTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).To(BeNil())

		Expect(migrationHistoryId).To(Equal(migrationUid))
	})

	It("Does not apply same migration twice", func() {
		migrationUid := utils.RandomString(8)
		migration := &migrations.Migration{MigrationDescription: description.MigrationDescription{ApplyTo: "a", Id: migrationUid}}

		_, err := NewMigrationManager(
			metaDescriptionSyncer, dbTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).To(BeNil())

		_, err = NewMigrationManager(
			metaDescriptionSyncer, dbTransactionManager,
		).recordAppliedMigration(migration)
		Expect(err).NotTo(BeNil())
	})
})
