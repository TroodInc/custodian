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
)

var _ = Describe("'RenameObject' Migration Operation", func() {
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
		metaDescription = description.GetBasicMetaDescription("random")

		operation := CreateObjectOperation{MetaDescription: metaDescription}
		globalTransaction, _ := globalTransactionManager.BeginTransaction(nil)
		metaDescription, err := operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(metaDescription).NotTo(BeNil())
	})

	It("renames metaDescription`s file", func() {
		updatedMetaDescription := metaDescription.Clone()
		updatedMetaDescription.Name = "b"

		operation := RenameObjectOperation{MetaDescription: updatedMetaDescription}
		globalTransaction, _ := globalTransactionManager.BeginTransaction(nil)
		updatedMetaDescription, err := operation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(updatedMetaDescription).NotTo(BeNil())

		//ensure MetaDescription has been save to file
		updatedMetaDescription, _, err = metaDescriptionSyncer.Get(updatedMetaDescription.Name)
		Expect(metaDescription).NotTo(BeNil())
		//ensure previous MetaDescription does not exist
		metaDescription, _, err = metaDescriptionSyncer.Get(metaDescription.Name)
		Expect(metaDescription).To(BeNil())

		//clean up
		_, err = metaDescriptionSyncer.Remove(updatedMetaDescription.Name)
		Expect(err).To(BeNil())
	})

	It("does not rename metaDescription if new name clashes with the existing one", func() {
		bMetaDescription := &description.MetaDescription{
			Name: "b",
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
		createOperation := CreateObjectOperation{MetaDescription: bMetaDescription}
		globalTransaction, _ := globalTransactionManager.BeginTransaction(nil)
		bMetaDescription, err := createOperation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		Expect(bMetaDescription).NotTo(BeNil())

		//
		renameOperation := RenameObjectOperation{bMetaDescription}
		renamedMetaObj, err := renameOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		globalTransactionManager.CommitTransaction(globalTransaction)

		// Ensure migration has not been applied
		Expect(err).NotTo(BeNil())
		Expect(renamedMetaObj).To(BeNil())

		//clean up
		metaDescriptionSyncer.Remove(bMetaDescription.Name)
		metaDescriptionSyncer.Remove(metaDescription.Name)
	})
})
