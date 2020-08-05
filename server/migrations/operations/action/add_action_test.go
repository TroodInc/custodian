package action

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
)

var _ = Describe("'AddAction' Migration Operation", func() {
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
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	//setup MetaDescription
	BeforeEach(func() {
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
			},
		}
		globalTransaction, _ := globalTransactionManager.BeginTransaction(nil)
		err := metaDescriptionSyncer.Create(globalTransaction.MetaDescriptionTransaction, *metaDescription)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	//setup teardown
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("adds an action into metaDescription`s file", func() {

		action := description.Action{Name: "new_action", Method: description.MethodCreate, Protocol: description.REST, Args: []string{"http://localhost:3000/some-handler"}}

		operation := NewAddActionOperation(&action)
		globalTransaction, _ := globalTransactionManager.BeginTransaction(nil)
		objectMeta, err := operation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(objectMeta).NotTo(BeNil())

		//ensure MetaDescription contains added field
		Expect(objectMeta.FindAction("new_action")).NotTo(BeNil())
		//ensure MetaDescription has been save to file with new field
		metaDescription, _, err := metaDescriptionSyncer.Get(objectMeta.Name)
		Expect(metaDescription).NotTo(BeNil())
		Expect(metaDescription.Actions).To(HaveLen(1))
		Expect(metaDescription.Actions[0].Name).To(Equal("new_action"))

		//clean up
		metaDescriptionSyncer.Remove(metaDescription.Name)
	})
})
