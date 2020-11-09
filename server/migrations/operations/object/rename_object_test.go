package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"custodian/server/pg"
	"custodian/utils"
	"custodian/server/object/meta"
	"custodian/server/transactions/file_transaction"
	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions"
	"custodian/server/object/description"
	"custodian/server/pg_meta"
)

var _ = Describe("'RenameObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)	

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg_meta.NewPgMetaDescriptionSyncer(dbTransactionManager)
	
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	var metaDescription *description.MetaDescription

	//setup transaction
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	testObjAName := utils.RandomString(8)
	testObjBName := utils.RandomString(8)

	//setup MetaDescription
	BeforeEach(func() {
		metaDescription = &description.MetaDescription{
			Name: testObjAName,
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

		operation := CreateObjectOperation{MetaDescription: metaDescription}
		globalTransaction, _ := globalTransactionManager.BeginTransaction(nil)
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
		updatedMetaDescription.Name = testObjBName

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
			Name: testObjBName,
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
	It("renames metaDescription if only new name provided", func() {
		bMetaDescription := &description.MetaDescription{
			Name:   testObjBName,
			Key:    "id",
			Cas:    false,
			Fields: []description.Field{},
		}

		operation := RenameObjectOperation{MetaDescription: bMetaDescription}
		globalTransaction, _ := globalTransactionManager.BeginTransaction(nil)
		bMetaDescription, err := operation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(bMetaDescription).NotTo(BeNil())

		//ensure MetaDescription has been save to file
		bMetaDescription, _, err = metaDescriptionSyncer.Get(bMetaDescription.Name)
		Expect(metaDescription).NotTo(BeNil())
		//ensure previous MetaDescription does not exist
		metaDescription, _, err = metaDescriptionSyncer.Get(metaDescription.Name)
		Expect(metaDescription).To(BeNil())

		//clean up
		_, err = metaDescriptionSyncer.Remove(bMetaDescription.Name)
		Expect(err).To(BeNil())
	})
})
