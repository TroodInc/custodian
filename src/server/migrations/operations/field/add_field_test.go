package field

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("'AddField' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := object.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	var metaDescription *object.Meta

	//setup MetaDescription
	BeforeEach(func() {
		metaDescription = &object.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
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

	It("adds a field into metaDescription`s file", func() {

		field := object.Field{Name: "new_field", Type: object.FieldTypeString, Optional: true}

		operation := NewAddFieldOperation(&field)
		globalTransaction, _ := globalTransactionManager.BeginTransaction()
		objectMeta, err := operation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(objectMeta).NotTo(BeNil())

		//ensure MetaDescription contains added field
		Expect(objectMeta.FindField("new_field")).NotTo(BeNil())
		//ensure MetaDescription has been save to file with new field
		metaMap, _, err := metaDescriptionSyncer.Get(objectMeta.Name)
		metaDescription = object.NewMetaFromMap(metaMap)
		Expect(metaDescription).NotTo(BeNil())
		Expect(metaDescription.Fields).To(HaveLen(2))
		Expect(metaDescription.Fields[1].Name).To(Equal("new_field"))

		//clean up
		metaDescriptionSyncer.Remove(metaDescription.Name)
	})
})
