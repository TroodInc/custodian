package action

import (
	"custodian/server/noti"
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'UpdateAction' Migration Operation", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)

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
					Method:   description.MethodCreate,
					Protocol: noti.REST,
					Args:     []string{"http://localhost:3000/some-handler"},
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
			Protocol: noti.REST,
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
