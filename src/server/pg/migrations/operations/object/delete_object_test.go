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

var _ = Describe("'DeleteObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
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
		//sync its MetaDescription
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

	It("removes MetaDescription`s file", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		//remove Meta from DB
		metaName := metaObj.Name
		err = new(DeleteObjectOperation).SyncDbDescription(metaObj, globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)

		//ensure table has been removed
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaName)
		Expect(err).NotTo(BeNil())
		Expect(metaDdlFromDB).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)

		//	ensure meta file does not exist
		metaDescription, _, err := metaDescriptionSyncer.Get(metaName)
		Expect(metaDescription).To(BeNil())
	})
})