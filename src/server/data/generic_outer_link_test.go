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
	"server/data/record"
	"fmt"
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
		err := metaStore.Flush(globalTransaction)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	Describe("Querying records with generic fields` values", func() {

		var aRecord *record.Record
		var bRecord *record.Record
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
						Name:     "name",
						Type:     description.FieldTypeString,
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
			(&description.NormalizationService{}).Normalize(&bMetaDescription)
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
			(&description.NormalizationService{}).Normalize(&aMetaDescription)
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(globalTransaction, aMetaObj.Name, aMetaObj, true)
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectA := func() {
			aRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "a", map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectA := func() {
			bRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "b", map[string]interface{}{"target": map[string]interface{}{"_object": "a", "id": aRecord.Data["id"]}, "name": "brecord"}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("can retrieve record with outer generic field as key", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having object A containing outer field referencing object B", havingObjectAWithGenericOuterLinkToB)
			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			aRecord, err := dataProcessor.Get(globalTransaction.DbTransaction, "a", strconv.Itoa(int(aRecord.Data["id"].(float64))), 1, false)
			Expect(err).To(BeNil())
			bSet := aRecord.Data["b_set"].([]interface{})
			Expect(bSet).To(HaveLen(1))
			Expect(bSet).To(Equal([]interface{}{bRecord.Data["id"].(float64)}))
		})

		It("can retrieve record with outer generic field as object", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having object A containing outer field referencing object B", havingObjectAWithGenericOuterLinkToB)
			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			aRecord, err := dataProcessor.Get(globalTransaction.DbTransaction, "a", strconv.Itoa(int(aRecord.Data["id"].(float64))), 3, false)
			Expect(err).To(BeNil())
			bSet := aRecord.Data["b_set"].([]interface{})
			Expect(bSet).To(HaveLen(1))
			targetValue := bSet[0].(map[string]interface{})["target"].(map[string]interface{})
			Expect(targetValue[types.GenericInnerLinkObjectKey].(string)).To(Equal("a"))
		})

		It("can create record with nested records referenced by outer generic link, referenced record does not exist", func() {
			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			havingObjectAWithGenericOuterLinkToB()

			aRecordData := map[string]interface{}{"name": "New A record", "b_set": []interface{}{map[string]interface{}{}}}
			aRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, "a", aRecordData, auth.User{})

			Expect(err).To(BeNil())
			Expect(aRecord.Data).To(HaveKey("b_set"))
			Expect(aRecord.Data["b_set"]).To(HaveLen(1))
			bSetData := aRecord.Data["b_set"].([]interface{})
			Expect(bSetData[0]).To(BeAssignableToTypeOf(1.0))

		})

		It("can create record with nested records referenced by outer generic link, referenced record exists", func() {
			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having object A with manually set outer link to B", havingObjectAWithGenericOuterLinkToB)

			bRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, "b", map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			aRecordData := map[string]interface{}{"name": "New A record", "b_set": []interface{}{map[string]interface{}{"id": bRecord.Data["id"]}}}
			aRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, "a", aRecordData, auth.User{})

			//check returned data
			Expect(err).To(BeNil())
			Expect(aRecord.Data).To(HaveKey("b_set"))
			Expect(aRecord.Data["b_set"]).To(HaveLen(1))
			bSetData := aRecord.Data["b_set"].([]interface{})
			Expect(bSetData[0].(float64)).To(Equal(bRecord.Data["id"]))

			//check queried data
			aRecord, err = dataProcessor.Get(globalTransaction.DbTransaction, "a", strconv.Itoa(int(aRecord.Data["id"].(float64))), 1, false)
			Expect(err).To(BeNil())

			Expect(aRecord.Data).To(HaveKey("b_set"))
			Expect(aRecord.Data["b_set"]).To(HaveLen(1))
			bSetData = aRecord.Data["b_set"].([]interface{})
			Expect(bSetData[0].(float64)).To(Equal(bRecord.Data["id"]))
		})

		It("can query records by outer generic field value", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having object A containing outer field referencing object B", havingObjectAWithGenericOuterLinkToB)
			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			bRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "b", map[string]interface{}{"target": map[string]interface{}{"_object": "a", "id": aRecord.Data["id"]}, "name": "anotherbrecord"}, auth.User{})
			Expect(err).To(BeNil())

			matchedRecords := []map[string]interface{}{}
			callbackFunction := func(obj map[string]interface{}) error {
				matchedRecords = append(matchedRecords, obj)
				return nil
			}
			_, err := dataProcessor.GetBulk(globalTransaction.DbTransaction, "a", fmt.Sprintf("eq(b_set.name,%s)", bRecord.Data["name"].(string)), nil, nil, 1, false, callbackFunction)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
		})
	})
})
