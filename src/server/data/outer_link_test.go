package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/description"
	"server/object/meta"
	"server/pg"
	"server/data"
	"utils"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/auth"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	BeforeEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	Describe("Data retrieve depending on outer link modes values", func() {
		havingObjectA := func() *meta.Meta {
			aMetaDescription := description.GetBasicMetaDescription("random")
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())
			return aMetaObj
		}

		havingObjectBLinkedToA := func(A *meta.Meta) *meta.Meta {
			bMetaDescription := description.GetBasicMetaDescription("random")
			bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
				Name:     "a",
				Type:     description.FieldTypeObject,
				LinkType: description.LinkTypeInner,
				LinkMeta: A.Name,
				Optional: false,
			})
			bMetaObj, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())
			return bMetaObj
		}

		havingObjectAWithManuallySpecifiedOuterLinkToB := func(A *meta.Meta, B *meta.Meta) *meta.Meta {
			aMetaDescription := description.GetBasicMetaDescription(A.Name)
			aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
				Name:           "b_set",
				Type:           description.FieldTypeArray,
				LinkType:       description.LinkTypeOuter,
				LinkMeta:       B.Name,
				OuterLinkField: "a",
				Optional:       true,
			})
			(&description.NormalizationService{}).Normalize(aMetaDescription)
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
			Expect(err).To(BeNil())
			return aMetaObj
		}

		It("retrieves data if outer link RetrieveMode is on", func() {

			A := havingObjectA()
			B := havingObjectBLinkedToA(A)
			objectA := havingObjectAWithManuallySpecifiedOuterLinkToB(A, B)

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
