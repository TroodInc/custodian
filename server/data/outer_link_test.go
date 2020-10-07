package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/pg"
	"custodian/server/data"
	"custodian/utils"
	"custodian/server/transactions/file_transaction"
	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions"
	"custodian/server/auth"
	"custodian/server/pg_meta"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg_meta.NewPgMetaDescriptionSyncer(dbTransactionManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	Describe("Data retrieve depending on outer link modes values", func() {
		havingObjectA := func() *meta.Meta {
			aMetaDescription := description.MetaDescription{
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
				Name: "b",
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
						Name:     "a",
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: "a",
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
						Optional: true,
					},
					{
						Name:           "b_set",
						Type:           description.FieldTypeArray,
						LinkType:       description.LinkTypeOuter,
						LinkMeta:       "b",
						OuterLinkField: "a",
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
