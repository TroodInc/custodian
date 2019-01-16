package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"utils"
	"server/object/meta"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/object/description"
	"database/sql"
)

var _ = Describe("'CreateObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer.Remove, metaDescriptionSyncer.Create)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	var globalTransaction *transactions.GlobalTransaction

	var metaDescription *description.MetaDescription

	//setup transaction
	BeforeEach(func() {
		var err error

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	//setup MetaDescription
	BeforeEach(func() {
		metaDescription = &description.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
	})

	//setup teardown
	AfterEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("creates corresponding table in the database", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		operation := NewCreateObjectOperation(metaDescription)
		metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		//sync MetaDescription with DB
		err = operation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)

		//ensure table has been created
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())

		globalTransactionManager.RollbackTransaction(globalTransaction)
	})
})
