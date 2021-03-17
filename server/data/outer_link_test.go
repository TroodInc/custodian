package data_test

import (
	"custodian/server/auth"
	"custodian/server/data"
	"custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/transactions"
	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(dbTransactionManager)
	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(globalTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	Describe("Data retrieve depending on outer link modes values", func() {

		testObjAName := utils.RandomString(8)
		testObjBName := utils.RandomString(8)

		havingObjectA := func() *meta.Meta {
			aMetaDescription := description.MetaDescription{
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
						Optional: true,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())
			return aMetaObj
		}

		havingObjectBLinkedToA := func() *meta.Meta {
			bMetaDescription := description.MetaDescription{
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
						Optional: true,
					},
					{
						Name:     testObjAName,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjAName,
						Optional: false,
					},
				},
			}
			bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())
			return bMetaObj
		}

		havingObjectAWithManuallySpecifiedOuterLinkToB := func() *meta.Meta {
			aMetaDescription := description.MetaDescription{
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
						Optional: true,
					},
					{
						Name:           "b_set",
						Type:           description.FieldTypeArray,
						LinkType:       description.LinkTypeOuter,
						LinkMeta:       testObjBName,
						OuterLinkField: testObjAName,
						Optional:       true,
					},
				},
			}
			(&description.NormalizationService{}).Normalize(&aMetaDescription)
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
			Expect(err).To(BeNil())
			return aMetaObj
		}

		It("retrieves data if outer link RetrieveMode is on", func() {

			havingObjectA()
			havingObjectBLinkedToA()
			objectA := havingObjectAWithManuallySpecifiedOuterLinkToB()

			aRecord, err := dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			aRecord, err = dataProcessor.Get(objectA.Name, aRecord.PkAsString(), nil, nil, 1, false)
			Expect(aRecord.Data).To(HaveKey("b_set"))
		})

		It("does not retrieve data if outer link RetrieveMode is off", func() {

			objectA := havingObjectA()
			havingObjectBLinkedToA()

			aRecord, err := dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			aRecord, err = dataProcessor.Get(objectA.Name, aRecord.PkAsString(), nil, nil, 1, false)
			Expect(aRecord.Data).NotTo(HaveKey("b_set"))
		})
	})
})
