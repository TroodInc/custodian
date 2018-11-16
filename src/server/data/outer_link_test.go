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
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	var globalTransaction *transactions.GlobalTransaction

	BeforeEach(func() {
		var err error

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

	})

	AfterEach(func() {
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
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
			err = metaStore.Create(globalTransaction, aMetaObj)
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
			err = metaStore.Create(globalTransaction, bMetaObj)
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
			_, err = metaStore.Update(globalTransaction, aMetaObj.Name, aMetaObj, true)
			Expect(err).To(BeNil())
			return aMetaObj
		}

		It("retrieves data if outer link RetrieveMode is on", func() {

			havingObjectA()
			havingObjectBLinkedToA()
			objectA := havingObjectAWithManuallySpecifiedOuterLinkToB()

			aRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectA.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			aRecord, err = dataProcessor.Get(globalTransaction.DbTransaction, objectA.Name, aRecord.PkAsString(), 1)
			Expect(aRecord.Data).To(HaveKey("b_set"))
		})

		It("does not retrieve data if outer link RetrieveMode is off", func() {

			objectA := havingObjectA()
			havingObjectBLinkedToA()

			aRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectA.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			aRecord, err = dataProcessor.Get(globalTransaction.DbTransaction, objectA.Name, aRecord.PkAsString(), 1)
			Expect(aRecord.Data).NotTo(HaveKey("b_set"))
		})
	})
})
