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

var _ = Describe("'RenameObject' Migration Operation", func() {
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

	var metaObj *meta.Meta

	//setup transaction
	BeforeEach(func() {
		var err error

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	//setup Meta
	BeforeEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		metaDescription := description.MetaDescription{
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
		Expect(err).To(BeNil())
		//factory new Meta
		metaObj, err = new(meta.MetaFactory).FactoryMeta(&metaDescription)
		Expect(err).To(BeNil())

		//sync its MetaDescription with MetaDescription storage
		err = metaDescriptionSyncer.Create(globalTransaction.MetaDescriptionTransaction, metaDescription)
		Expect(err).To(BeNil())
		//sync its MetaDescription with DB
		err = syncer.CreateObj(globalTransaction.DbTransaction, metaObj)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	//setup teardown
	AfterEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("renames corresponding table in the database", func() {
		oldMetaName := metaObj.Name
		newMetaName := "b"
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		//sync Meta with DB
		operation := NewRenameObjectOperation(newMetaName)
		err = operation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
		Expect(err).To(BeNil())
		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)

		//ensure table has been renamed
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, newMetaName)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())

		//ensure table with old name does not exist
		oldMetaDdlFromDB, err := pg.MetaDDLFromDB(tx, oldMetaName)
		Expect(err).NotTo(BeNil())
		Expect(oldMetaDdlFromDB).To(BeNil())

		globalTransactionManager.RollbackTransaction(globalTransaction)
	})
})