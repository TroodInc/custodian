package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/description"
	"server/object/meta"
	"server/pg"
	"server/data"
	"utils"
	"server/auth"
	"strconv"
	"server/data/types"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

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

	Describe("Querying records by generic fields` values", func() {

		var aRecordData map[string]interface{}
		var bRecord map[string]interface{}
		var err error

		havingObjectA := func() {
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
						Name:     "name",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, aMetaObj)
			Expect(err).To(BeNil())
		}

		havingObjectBWithGenericLinkToA := func() {
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
						Name:         "target",
						Type:         description.FieldTypeGeneric,
						LinkType:     description.LinkTypeInner,
						LinkMetaList: []string{"a"},
						Optional:     true,
					},
				},
			}
			bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, bMetaObj)
			Expect(err).To(BeNil())
		}

		havingObjectAWithGenericOuterLinkToB := func() {
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
						Name:     "name",
						Type:     description.FieldTypeString,
						Optional: false,
					},
					{
						Name:           "b_set",
						Type:           description.FieldTypeGeneric,
						LinkType:       description.LinkTypeOuter,
						LinkMeta:       "b",
						OuterLinkField: "target",
						Optional:       true,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(globalTransaction, aMetaObj.Name, aMetaObj, true)
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectA := func() {
			aRecordData, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "a", map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectA := func() {
			bRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "b", map[string]interface{}{"target": map[string]interface{}{"_object": "a", "id": aRecordData["id"]}}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("can retrieve record with outer generic field as key", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having object A containing outer field referencing object B", havingObjectAWithGenericOuterLinkToB)
			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			aRecord, err := dataProcessor.Get(globalTransaction.DbTransaction, "a", strconv.Itoa(int(aRecordData["id"].(float64))), 1)
			Expect(err).To(BeNil())
			bSet := aRecord.Data["b_set"].([]interface{})
			Expect(bSet).To(HaveLen(1))
			Expect(bSet).To(Equal([]interface{}{bRecord["id"].(float64)}))
		})

		It("can retrieve record with outer generic field as object", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having object A containing outer field referencing object B", havingObjectAWithGenericOuterLinkToB)
			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			aRecord, err := dataProcessor.Get(globalTransaction.DbTransaction, "a", strconv.Itoa(int(aRecordData["id"].(float64))), 3)
			Expect(err).To(BeNil())
			bSet := aRecord.Data["b_set"].([]interface{})
			Expect(bSet).To(HaveLen(1))
			targetValue := bSet[0].(map[string]interface{})["target"].(map[string]interface{})
			Expect(targetValue[types.GenericInnerLinkObjectKey].(string)).To(Equal("a"))
		})
	})
})
