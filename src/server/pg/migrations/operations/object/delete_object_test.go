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
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	var metaDescription *description.MetaDescription

	//setup transaction
	BeforeEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())

	//setup MetaDescription
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
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
		Expect(err).To(BeNil())
		//sync its MetaDescription
		err = syncer.CreateObj(globalTransaction.DbTransaction, metaDescription, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("removes MetaDescription`s file", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		//remove MetaDescription from DB
		metaName := metaDescription.Name
		err = new(DeleteObjectOperation).SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
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
