package action

import (
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'RemoveAction' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := object.NewStore(metaDescriptionSyncer, syncer, dbTransactionManager)

	var metaDescription *description.MetaDescription

	//setup transaction
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

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
					Method:   description.MethodCreate,
					Protocol: description.REST,
					Args:     []string{"http://localhost:3000/some-handler"},
				},
			},
		}
		err := metaDescriptionSyncer.Create(*metaDescription)
		Expect(err).To(BeNil())
	})

	It("removes an action from metaDescription`s file", func() {
		operation := NewRemoveActionOperation(metaDescription.FindAction("new_action"))
		objectMeta, err := operation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		Expect(objectMeta).NotTo(BeNil())

		//ensure action has been removed from file
		metaDescription, _, err := metaDescriptionSyncer.Get(objectMeta.Name)
		Expect(metaDescription).NotTo(BeNil())
		Expect(metaDescription.Actions).To(HaveLen(0))

		//clean up
		metaDescriptionSyncer.Remove(metaDescription.Name)
	})
})
