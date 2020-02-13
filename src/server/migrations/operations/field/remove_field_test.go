package field

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/meta"

	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("'RemoveField' Migration Operation", func() {
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
				{
					Name:     "name",
					Type:     meta.FieldTypeString,
					Optional: true,
					Def:      "empty",
				},
			},
		}
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

	It("removes a field from metaDescription`s file", func() {
		operation := NewRemoveFieldOperation(metaDescription.FindField("name"))
		globalTransaction, _ := globalTransactionManager.BeginTransaction()
		objectMeta, err := operation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(objectMeta).NotTo(BeNil())

		//ensure MetaDescription has been removed from file
		metaMap, _, err := metaDescriptionSyncer.Get(objectMeta.Name)
		metaDescription = meta.NewMetaFromMap(metaMap)
		Expect(metaDescription).NotTo(BeNil())
		Expect(metaDescription.Fields).To(HaveLen(1))
		Expect(metaDescription.Fields[0].Name).To(Equal("id"))

		//clean up
		metaDescriptionSyncer.Remove(metaDescription.Name)
	})
})
