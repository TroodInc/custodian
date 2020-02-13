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

var _ = Describe("'UpdateField' Migration Operation", func() {
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

	//setup transaction
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

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
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		err = metaDescriptionSyncer.Create(globalTransaction.MetaDescriptionTransaction, metaDescription.Name, metaDescription.ForExport())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
	})

	It("replaces a field in metaDescription`s file", func() {

		field := meta.Field{Name: "name", Type: meta.FieldTypeNumber, Optional: false, Def: nil}

		operation := NewUpdateFieldOperation(metaDescription.FindField("name"), &field)
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		metaDescription, err := operation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
		Expect(metaDescription).NotTo(BeNil())

		//ensure MetaDescription has been save to file with new field
		description, _, err := metaDescriptionSyncer.Get(metaDescription.Name)
		metaDescription = meta.NewMetaFromMap(description)
		Expect(metaDescription).NotTo(BeNil())
		Expect(metaDescription.Fields).To(HaveLen(2))
		Expect(metaDescription.Fields[1].Name).To(Equal("name"))

		Expect(metaDescription.FindField("name").Optional).To(BeFalse())
		Expect(metaDescription.FindField("name").Def).To(BeNil())
		Expect(metaDescription.FindField("name").Type).To(Equal(meta.FieldTypeNumber))

		//clean up
		metaDescriptionSyncer.Remove(metaDescription.Name)
	})
})
