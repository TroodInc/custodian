package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/meta"

	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("'DeleteObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewMetaStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	var metaDescription *meta.Meta


	//setup MetaDescription
	BeforeEach(func() {
		metaDescription = &meta.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}

		//factory new MetaDescription
		//sync its MetaDescription
		globalTransaction, _ := globalTransactionManager.BeginTransaction()
		err := metaDescriptionSyncer.Create(globalTransaction.MetaDescriptionTransaction, metaDescription.Name, metaDescription.ForExport())
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	//setup teardown
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("removes MetaDescription`s file", func() {
		operation := DeleteObjectOperation{}
		metaName := metaDescription.Name
		globalTransaction, _ := globalTransactionManager.BeginTransaction()
		metaObj, err := operation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(metaObj).To(BeNil())

		//ensure meta`s file has been removed
		metaDescription, _, err := metaDescriptionSyncer.Get(metaName)
		Expect(metaDescription).To(BeNil())
		Expect(err).NotTo(BeNil())
	})
})
