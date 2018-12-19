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

	//setup MetaDescription
	BeforeEach(func() {
		metaDescription := &description.MetaDescription{
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
		metaOjb, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(metaDescription)
		Expect(err).To(BeNil())

		operation := CreateObjectOperation{Meta: metaOjb}

		metaObj, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		Expect(metaObj).NotTo(BeNil())
	})

	//setup teardown
	AfterEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("renames metaDescription`s file", func() {
		updatedMetaDescription := metaObj.MetaDescription.Clone()
		updatedMetaDescription.Name = "b"
		updatedMetaObj, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(updatedMetaDescription)
		Expect(err).To(BeNil())

		operation := RenameObjectOperation{Meta: updatedMetaObj}
		updatedMetaObj, err = operation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		Expect(metaObj).NotTo(BeNil())

		//ensure MetaDescription has been save to file
		metaDescription, _, err := metaDescriptionSyncer.Get(updatedMetaObj.Name)
		Expect(metaDescription).NotTo(BeNil())
		//ensure previous MetaDescription does not exist
		metaDescription, _, err = metaDescriptionSyncer.Get(metaObj.Name)
		Expect(metaDescription).To(BeNil())

		//clean up
		_, err = metaDescriptionSyncer.Remove(updatedMetaObj.Name)
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

		bMetaOjb, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(bMetaDescription)
		Expect(err).To(BeNil())

		createOperation := CreateObjectOperation{Meta: bMetaOjb}
		bMetaObj, err := createOperation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		Expect(metaObj).NotTo(BeNil())

		//
		renameOperation := RenameObjectOperation{bMetaObj}
		renamedMetaObj, err := renameOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)

		// Ensure migration has not been applied
		Expect(err).NotTo(BeNil())
		Expect(renamedMetaObj).To(BeNil())

		//clean up
		metaDescriptionSyncer.Remove(bMetaObj.Name)
		metaDescriptionSyncer.Remove(metaObj.Name)
	})
})
