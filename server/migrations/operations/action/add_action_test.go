package action

import (
	"custodian/server/noti"
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'AddAction' Migration Operation", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache())
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)

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
		}
		err := metaDescriptionSyncer.Create(*metaDescription)
		Expect(err).To(BeNil())
	})

	//setup teardown
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("adds an action into metaDescription`s file", func() {

		action := description.Action{Name: "new_action", Method: description.MethodCreate, Protocol: noti.REST, Args: []string{"http://localhost:3000/some-handler"}}

		operation := NewAddActionOperation(&action)
		objectMeta, err := operation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
		Expect(err).To(BeNil())
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
