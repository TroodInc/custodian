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

var _ = Describe("'RenameObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &transactions.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

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
			},
		}

		operation := CreateObjectOperation{MetaDescription: metaDescription}
		globalTransaction, _ := globalTransactionManager.BeginTransaction()
		metaDescription, err := operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(metaDescription).NotTo(BeNil())
	})

	//setup teardown
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("renames metaDescription`s file", func() {
		updatedMetaDescription := metaDescription.Clone()
		updatedMetaDescription.Name = "b"

		operation := RenameObjectOperation{MetaDescription: updatedMetaDescription}
		globalTransaction, _ := globalTransactionManager.BeginTransaction()
		updatedMetaDescription, err := operation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(updatedMetaDescription).NotTo(BeNil())

		//ensure MetaDescription has been save to file
		updatedMetaMap, _, err := metaDescriptionSyncer.Get(updatedMetaDescription.Name)
		Expect(updatedMetaMap).NotTo(BeNil())
		//ensure previous MetaDescription does not exist
		metaMap, _, err := metaDescriptionSyncer.Get(metaDescription.Name)
		Expect(metaMap).To(BeNil())

		//clean up
		_, err = metaDescriptionSyncer.Remove(updatedMetaDescription.Name)
		Expect(err).To(BeNil())
	})

	It("does not rename metaDescription if new name clashes with the existing one", func() {
		bMetaDescription := &meta.Meta{
			Name: "b",
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
		createOperation := CreateObjectOperation{MetaDescription: bMetaDescription}
		globalTransaction, _ := globalTransactionManager.BeginTransaction()
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
