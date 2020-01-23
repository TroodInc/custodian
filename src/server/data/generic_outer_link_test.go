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

	Describe("Querying records with generic fields` values", func() {

		var aRecord *record.Record
		var bRecord *record.Record
		var err error

		havingObjectA := func() *description.MetaDescription {
			aMetaDescription := description.GetBasicMetaDescription("random")
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())
			return aMetaDescription
		}

		havingObjectBWithGenericLinkToA := func(A *description.MetaDescription) *description.MetaDescription {
			bMetaDescription := description.GetBasicMetaDescription("random")
			bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
				Name:         "target",
				Type:         description.FieldTypeGeneric,
				LinkType:     description.LinkTypeInner,
				LinkMetaList: []string{A.Name},
				Optional:     true,
			})

			(&description.NormalizationService{}).Normalize(bMetaDescription)
			bMetaObj, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())

			return bMetaDescription
		}

		havingObjectAWithGenericOuterLinkToB := func(A *description.MetaDescription, B *description.MetaDescription) {
			A.Fields =  append(A.Fields, description.Field{
				Name:           "b_set",
				Type:           description.FieldTypeGeneric,
				LinkType:       description.LinkTypeOuter,
				LinkMeta:       B.Name,
				OuterLinkField: "target",
				Optional:       true,
			})

			(&description.NormalizationService{}).Normalize(A)
			aMetaObj, err := metaStore.NewMeta(A)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectA := func(A *description.MetaDescription) {
			aRecord, err = dataProcessor.CreateRecord(A.Name, map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectA := func(A *description.MetaDescription, B *description.MetaDescription) {
			bRecord, err = dataProcessor.CreateRecord(B.Name, map[string]interface{}{"target": map[string]interface{}{"_object": A.Name, "id": aRecord.Data["id"]}, "name": "brecord"}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("can retrieve record with outer generic field as key", func() {
			aMeta := havingObjectA()
			bMeta := havingObjectBWithGenericLinkToA(aMeta)
			havingObjectAWithGenericOuterLinkToB(aMeta, bMeta)
			By("And having a record of object A")
			havingARecordOfObjectA(aMeta)
			By("and having a record of object B containing generic field value with A object`s record")
			havingARecordOfObjectBContainingRecordOfObjectA(aMeta, bMeta)

			aRecord, err := dataProcessor.Get(aMeta.Name, strconv.Itoa(int(aRecord.Data["id"].(float64))), nil, nil, 1, false)
			Expect(err).To(BeNil())
			bSet := aRecord.Data["b_set"].([]interface{})
			Expect(bSet).To(HaveLen(1))
			Expect(bSet).To(Equal([]interface{}{bRecord.Data["id"].(float64)}))
		})

		It("can retrieve record with outer generic field as object", func() {
			aMeta := havingObjectA()
			bMeta := havingObjectBWithGenericLinkToA(aMeta)
			havingObjectAWithGenericOuterLinkToB(aMeta, bMeta)
			By("And having a record of object A")
			havingARecordOfObjectA(aMeta)
			By("and having a record of object B containing generic field value with A object`s record")
			havingARecordOfObjectBContainingRecordOfObjectA(aMeta, bMeta)

			aRecord, err := dataProcessor.Get(aMeta.Name, strconv.Itoa(int(aRecord.Data["id"].(float64))), nil, nil, 3, false)
			Expect(err).To(BeNil())
			bSet := aRecord.Data["b_set"].([]interface{})
			Expect(bSet).To(HaveLen(1))
			targetValue := bSet[0].(*record.Record).Data["target"].(*record.Record)
			Expect(targetValue.Data[types.GenericInnerLinkObjectKey].(string)).To(Equal(aMeta.Name))
		})

		It("can create record with nested records referenced by outer generic link, referenced record does not exist", func() {
			aMeta := havingObjectA()
			bMeta := havingObjectBWithGenericLinkToA(aMeta)
			havingObjectAWithGenericOuterLinkToB(aMeta, bMeta)

			aRecordData := map[string]interface{}{"name": "New A record", "b_set": []interface{}{map[string]interface{}{}}}
			aRecord, err := dataProcessor.CreateRecord(aMeta.Name, aRecordData, auth.User{})

			Expect(err).To(BeNil())
			Expect(aRecord.Data).To(HaveKey("b_set"))
			Expect(aRecord.Data["b_set"]).To(HaveLen(1))
			bSetData := aRecord.Data["b_set"].([]interface{})
			Expect(bSetData[0]).To(BeAssignableToTypeOf(1.0))

		})

		It("can create record with nested records referenced by outer generic link, referenced record exists", func() {
			aMeta := havingObjectA()
			bMeta := havingObjectBWithGenericLinkToA(aMeta)
			havingObjectAWithGenericOuterLinkToB(aMeta, bMeta)

			bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			aRecordData := map[string]interface{}{"name": "New A record", "b_set": []interface{}{map[string]interface{}{"id": bRecord.Data["id"]}}}
			aRecord, err := dataProcessor.CreateRecord( aMeta.Name, aRecordData, auth.User{})

			//check returned data
			Expect(err).To(BeNil())
			Expect(aRecord.Data).To(HaveKey("b_set"))
			Expect(aRecord.Data["b_set"]).To(HaveLen(1))
			bSetData := aRecord.Data["b_set"].([]interface{})
			Expect(bSetData[0].(float64)).To(Equal(bRecord.Data["id"]))

			//check queried data
			aRecord, err = dataProcessor.Get( aMeta.Name, strconv.Itoa(int(aRecord.Data["id"].(float64))), nil, nil, 1, false)
			Expect(err).To(BeNil())

			Expect(aRecord.Data).To(HaveKey("b_set"))
			Expect(aRecord.Data["b_set"]).To(HaveLen(1))
			bSetData = aRecord.Data["b_set"].([]interface{})
			Expect(bSetData[0].(float64)).To(Equal(bRecord.Data["id"]))
		})

		XIt("can query records by outer generic field value", func() {
			aMeta := havingObjectA()
			bMeta := havingObjectBWithGenericLinkToA(aMeta)
			havingObjectAWithGenericOuterLinkToB(aMeta, bMeta)
			By("And having a record of object A")
			havingARecordOfObjectA(aMeta)
			By("and having a record of object B containing generic field value with A object`s record")
			havingARecordOfObjectBContainingRecordOfObjectA(aMeta, bMeta)

			bRecord, err = dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{"target": map[string]interface{}{"_object": aMeta.Name, "id": aRecord.Data["id"]}, "name": "anotherbrecord"}, auth.User{})
			Expect(err).To(BeNil())

			_, matchedRecords, err := dataProcessor.GetBulk(aMeta.Name, fmt.Sprintf("eq(b_set.name,%s)", bRecord.Data["name"].(string)), nil, nil, 1, false)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
		})
	})
})
