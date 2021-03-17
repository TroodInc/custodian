package object

import (
	"custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/transactions"
	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'DeleteObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(dbTransactionManager)
	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(globalTransactionManager)
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
		}

		//factory new MetaDescription
		//sync its MetaDescription
		err := metaDescriptionSyncer.Create(*metaDescription)
		Expect(err).To(BeNil())
	})

	//setup teardown
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("removes MetaDescription`s file", func() {
		operation := DeleteObjectOperation{}
		metaName := metaDescription.Name
		metaObj, err := operation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		Expect(metaObj).To(BeNil())

		//ensure meta`s file has been removed
		metaDescription, _, err := metaDescriptionSyncer.Get(metaName)
		Expect(metaDescription).To(BeNil())
		Expect(err).NotTo(BeNil())
	})
})
