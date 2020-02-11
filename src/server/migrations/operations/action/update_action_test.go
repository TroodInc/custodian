package action

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/data/notifications"
	"server/noti"
	"server/object"

	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("'UpdateAction' Migration Operation", func() {
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
			Actions: []*notifications.Action{
				{Name: "new_action",
					Method:   notifications.MethodCreate,
					Protocol: noti.REST,
					Args:     []string{"http://localhost:3000/some-handler"},
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

	It("replaces a field in metaDescription`s file", func() {
		currentAction := metaDescription.FindAction("new_action")

		newAction := &notifications.Action{
			Name:     "updated_action",
			Method:   notifications.MethodCreate,
			Protocol: noti.REST,
			Args:     []string{"http://localhost:3000/some-another-handler"},
		}

		operation := NewUpdateActionOperation(currentAction, newAction)
		globalTransaction, _ := globalTransactionManager.BeginTransaction()
		metaDescription, err := operation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(metaDescription).NotTo(BeNil())

		//ensure MetaDescription has been save to file with updated action
		description, _, err := metaDescriptionSyncer.Get(metaDescription.Name)
		Expect(description).NotTo(BeNil())
		Expect(metaDescription.Actions).To(HaveLen(1))
		Expect(metaDescription.Actions[0].Name).To(Equal("updated_action"))
		Expect(metaDescription.Actions[0].Args[0]).To(Equal("http://localhost:3000/some-another-handler"))

		//clean up
		metaDescriptionSyncer.Remove(metaDescription.Name)
	})
})
