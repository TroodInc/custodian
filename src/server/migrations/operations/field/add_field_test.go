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

var _ = Describe("'AddField' Migration Operation", func() {
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

	//setup MetaDescription
	BeforeEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())

		metaDescription = description.GetBasicMetaDescription("random")
		globalTransaction, _ := globalTransactionManager.BeginTransaction(nil)
		err = metaDescriptionSyncer.Create(globalTransaction.MetaDescriptionTransaction, *metaDescription)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("adds a field into metaDescription`s file", func() {

		field := description.Field{Name: "new_field", Type: description.FieldTypeString, Optional: true}

		operation := NewAddFieldOperation(&field)
		globalTransaction, _ := globalTransactionManager.BeginTransaction(nil)
		objectMeta, err := operation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(objectMeta).NotTo(BeNil())

		//ensure MetaDescription contains added field
		Expect(objectMeta.FindField("new_field")).NotTo(BeNil())
		//ensure MetaDescription has been save to file with new field
		metaDescription, _, err := metaDescriptionSyncer.Get(objectMeta.Name)
		Expect(metaDescription).NotTo(BeNil())
		Expect(metaDescription.Fields).To(HaveLen(2))
		Expect(metaDescription.Fields[1].Name).To(Equal("new_field"))

		//clean up
		metaDescriptionSyncer.Remove(metaDescription.Name)
	})
})
