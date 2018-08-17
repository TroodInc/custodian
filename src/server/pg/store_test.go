package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"utils"
	"server/transactions/file_transaction"

	pg_transactions "server/pg/transactions"
	"server/object/meta"
	"server/transactions"
	"server/object/description"
	"server/data"
	"server/auth"
)

var _ = Describe("Store", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	var globalTransaction *transactions.GlobalTransaction

	BeforeEach(func() {
		var err error

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

	})

	AfterEach(func() {
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("Having an record for person with null value", func() {
		//create meta
		meta := description.MetaDescription{
			Name: "person",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				}, {
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: false,
				}, {
					Name:     "gender",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}
		metaDescription, _ := metaStore.NewMeta(&meta)

		err := metaStore.Create(globalTransaction, metaDescription)
		Expect(err).To(BeNil())

		//create record
		recordData := map[string]interface{}{
			"name": "Sergey",
		}
		record, _ := dataProcessor.CreateRecord(globalTransaction.DbTransaction, meta.Name, recordData, auth.User{})
		Expect(record).To(HaveKey("gender"))
	})
})
