package field

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
)

var _ = Describe("'UpdateField' Migration Operation", func() {
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
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
					Def:      "empty",
				},
			},
		}
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		err = metaDescriptionSyncer.Create(globalTransaction.MetaDescriptionTransaction, *metaDescription)
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
	})

	It("replaces a field in metaDescription`s file", func() {

		field := description.Field{Name: "name", Type: description.FieldTypeNumber, Optional: false, Def: nil}

		operation := NewUpdateFieldOperation(metaDescription.FindField("name"), &field)
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		metaDescription, err := operation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
		Expect(metaDescription).NotTo(BeNil())

		//ensure MetaDescription has been save to file with new field
		metaDescription, _, err = metaDescriptionSyncer.Get(metaDescription.Name)
		Expect(metaDescription).NotTo(BeNil())
		Expect(metaDescription.Fields).To(HaveLen(2))
		Expect(metaDescription.Fields[1].Name).To(Equal("name"))

		Expect(metaDescription.FindField("name").Optional).To(BeFalse())
		Expect(metaDescription.FindField("name").Def).To(BeNil())
		Expect(metaDescription.FindField("name").Type).To(Equal(description.FieldTypeNumber))

		//clean up
		metaDescriptionSyncer.Remove(metaDescription.Name)
	})
})
