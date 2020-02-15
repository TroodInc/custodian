package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/auth"
	"server/data"
	"server/object"
	"server/object/driver"
	"server/object/meta"

	"server/pg"
	pg_transactions "server/pg/transactions"
	"utils"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := object.NewStore(driver)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		metaStore.Flush()
	})

	Describe("Data retrieve depending on outer link modes values", func() {
		havingObjectA := func() *meta.Meta {
			aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			return metaStore.Create(aMetaObj)
		}

		havingObjectBLinkedToA := func(A *meta.Meta) *meta.Meta {
			bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField(&meta.Field{
				Name:     "a",
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				LinkMeta: A,
				Optional: false,
			})
			bMetaObj, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			return metaStore.Create(bMetaObj)
		}

		havingObjectAWithManuallySpecifiedOuterLinkToB := func(B *meta.Meta) *meta.Meta {
			aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			aMetaDescription.AddField(&meta.Field{
				Name:           "b_set",
				Type:           meta.FieldTypeArray,
				LinkType:       meta.LinkTypeOuter,
				LinkMeta:       B,
				OuterLinkField: B.FindField("a"),
				Optional:       true,
			})
			(&meta.NormalizationService{}).Normalize(aMetaDescription)
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			return metaStore.Update(aMetaObj)
		}

		It("retrieves data if outer link RetrieveMode is on", func() {

			aMeta := havingObjectA()
			bMeta := havingObjectBLinkedToA(aMeta)
			objectA := havingObjectAWithManuallySpecifiedOuterLinkToB(bMeta)

			aRecord, err := dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			aRecord, err = dataProcessor.Get(objectA.Name, aRecord.PkAsString(), nil, nil, 1, false)
			Expect(aRecord.Data).To(HaveKey("b_set"))
		})

		It("does not retrieve data if outer link RetrieveMode is off", func() {

			objectA := havingObjectA()
			havingObjectBLinkedToA(objectA)

			aRecord, err := dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			aRecord, err = dataProcessor.Get(objectA.Name, aRecord.PkAsString(), nil, nil, 1, false)
			Expect(aRecord.Data).NotTo(HaveKey("b_set"))
		})
	})
})
