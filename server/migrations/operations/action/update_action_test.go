package action

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"custodian/server/pg"
	"custodian/utils"
	"custodian/server/object/meta"
	"custodian/server/transactions/file_transaction"
	"custodian/server/transactions"
	"custodian/server/object/description"
)

var _ = Describe("'UpdateAction' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg.NewPgMetaDescriptionSyncer(dbTransactionManager)

	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	var metaDescription *description.MetaDescription

	testObjAName := utils.RandomString(8)

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
			Actions: []description.Action{
				{Name: "new_action",
					Method: description.MethodCreate,
					Protocol: description.REST,
					Args: []string{"http://localhost:3000/some-handler"},
				},
			},
		}
		err := metaDescriptionSyncer.Create(*metaDescription)
		Expect(err).To(BeNil())
	})

	//setup teardown
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("replaces a field in metaDescription`s file", func() {
		currentAction := metaDescription.FindAction("new_action")

		newAction := &description.Action{
			Name:     "updated_action",
			Method:   description.MethodCreate,
			Protocol: description.REST,
			Args:     []string{"http://localhost:3000/some-another-handler"},
		}

		operation := NewUpdateActionOperation(currentAction, newAction)
		metaDescription, err := operation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		Expect(metaDescription).NotTo(BeNil())

		//ensure MetaDescription has been save to file with updated action
		metaDescription, _, err = metaDescriptionSyncer.Get(metaDescription.Name)
		Expect(metaDescription).NotTo(BeNil())
		Expect(metaDescription.Actions).To(HaveLen(1))
		Expect(metaDescription.Actions[0].Name).To(Equal("updated_action"))
		Expect(metaDescription.Actions[0].Args[0]).To(Equal("http://localhost:3000/some-another-handler"))

		//clean up
		metaDescriptionSyncer.Remove(metaDescription.Name)
	})
})
