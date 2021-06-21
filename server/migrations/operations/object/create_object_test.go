package object

import (
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'CreateObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)
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
	})

	It("stores MetaDescription to file", func() {

		operation := CreateObjectOperation{MetaDescription: metaDescription}
		metaDescription, err := operation.SyncMetaDescription(nil, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		Expect(metaDescription).NotTo(BeNil())

		//ensure MetaDescription has been save to file
		metaDescription, _, err = metaDescriptionSyncer.Get(metaDescription.Name)
		Expect(metaDescription).NotTo(BeNil())
		//clean up
		metaDescriptionSyncer.Remove(metaDescription.Name)
	})
})
