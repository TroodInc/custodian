package data_test

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/auth"
	"server/data"
	"server/data/record"
	"server/data/types"
	"server/object/meta"

	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"strconv"
	"utils"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	Describe("Querying records with generic fields` values", func() {

		var aRecord *record.Record
		var bRecord *record.Record
		var err error

		havingObjectA := func() *meta.Meta {
			aMetaDescription := meta.Meta{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []*meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
					{
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: false,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())

			return aMetaObj
		}

		havingObjectBWithGenericLinkToA := func(A *meta.Meta) *meta.Meta {
			bMetaDescription := meta.Meta{
				Name: "b",
				Key:  "id",
				Cas:  false,
				Fields: []*meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
					{
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: true,
					},
					{
						Name:         "target",
						Type:         meta.FieldTypeGeneric,
						LinkType:     meta.LinkTypeInner,
						LinkMetaList: []*meta.Meta{A},
						Optional:     true,
					},
				},
			}
			(&meta.NormalizationService{}).Normalize(&bMetaDescription)
			bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())

			return bMetaObj
		}

		havingObjectAWithGenericOuterLinkToB := func(B *meta.Meta) {
			aMetaDescription := meta.Meta{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []*meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
					{
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: false,
					},
					{
						Name:           "b_set",
						Type:           meta.FieldTypeGeneric,
						LinkType:       meta.LinkTypeOuter,
						LinkMeta:       B,
						OuterLinkField: B.FindField("target"),
						Optional:       true,
					},
				},
			}
			(&meta.NormalizationService{}).Normalize(&aMetaDescription)
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectA := func() {
			aRecord, err = dataProcessor.CreateRecord("a", map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectA := func() {
			bRecord, err = dataProcessor.CreateRecord("b", map[string]interface{}{"target": map[string]interface{}{"_object": "a", "id": aRecord.Data["id"]}, "name": "brecord"}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("can retrieve record with outer generic field as key", func() {

			aMeta := havingObjectA()
			bMeta := havingObjectBWithGenericLinkToA(aMeta)
			havingObjectAWithGenericOuterLinkToB(bMeta)
			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			aRecord, err := dataProcessor.Get("a", strconv.Itoa(int(aRecord.Data["id"].(float64))), nil, nil, 1, false)
			Expect(err).To(BeNil())
			bSet := aRecord.Data["b_set"].([]interface{})
			Expect(bSet).To(HaveLen(1))
			Expect(bSet).To(Equal([]interface{}{bRecord.Data["id"].(float64)}))
		})

		It("can retrieve record with outer generic field as object", func() {

			aMeta := havingObjectA()
			bMeta := havingObjectBWithGenericLinkToA(aMeta)
			havingObjectAWithGenericOuterLinkToB(bMeta)
			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			aRecord, err := dataProcessor.Get("a", strconv.Itoa(int(aRecord.Data["id"].(float64))), nil, nil, 3, false)
			Expect(err).To(BeNil())
			bSet := aRecord.Data["b_set"].([]interface{})
			Expect(bSet).To(HaveLen(1))
			targetValue := bSet[0].(*record.Record).Data["target"].(*record.Record)
			Expect(targetValue.Data[types.GenericInnerLinkObjectKey].(string)).To(Equal("a"))
		})

		It("can create record with nested records referenced by outer generic link, referenced record does not exist", func() {
			aMeta := havingObjectA()
			bMeta := havingObjectBWithGenericLinkToA(aMeta)
			havingObjectAWithGenericOuterLinkToB(bMeta)

			aRecordData := map[string]interface{}{"name": "New A record", "b_set": []interface{}{map[string]interface{}{}}}
			aRecord, err := dataProcessor.CreateRecord("a", aRecordData, auth.User{})

			Expect(err).To(BeNil())
			Expect(aRecord.Data).To(HaveKey("b_set"))
			Expect(aRecord.Data["b_set"]).To(HaveLen(1))
			bSetData := aRecord.Data["b_set"].([]interface{})
			Expect(bSetData[0]).To(BeAssignableToTypeOf(1.0))

		})

		It("can create record with nested records referenced by outer generic link, referenced record exists", func() {
			aMeta := havingObjectA()
			bMeta := havingObjectBWithGenericLinkToA(aMeta)
			havingObjectAWithGenericOuterLinkToB(bMeta)

			bRecord, err := dataProcessor.CreateRecord("b", map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			aRecordData := map[string]interface{}{"name": "New A record", "b_set": []interface{}{map[string]interface{}{"id": bRecord.Data["id"]}}}
			aRecord, err := dataProcessor.CreateRecord( "a", aRecordData, auth.User{})

			//check returned data
			Expect(err).To(BeNil())
			Expect(aRecord.Data).To(HaveKey("b_set"))
			Expect(aRecord.Data["b_set"]).To(HaveLen(1))
			bSetData := aRecord.Data["b_set"].([]interface{})
			Expect(bSetData[0].(float64)).To(Equal(bRecord.Data["id"]))

			//check queried data
			aRecord, err = dataProcessor.Get( "a", strconv.Itoa(int(aRecord.Data["id"].(float64))), nil, nil, 1, false)
			Expect(err).To(BeNil())

			Expect(aRecord.Data).To(HaveKey("b_set"))
			Expect(aRecord.Data["b_set"]).To(HaveLen(1))
			bSetData = aRecord.Data["b_set"].([]interface{})
			Expect(bSetData[0].(float64)).To(Equal(bRecord.Data["id"]))
		})

		It("can query records by outer generic field value", func() {

			aMeta := havingObjectA()
			bMeta := havingObjectBWithGenericLinkToA(aMeta)
			havingObjectAWithGenericOuterLinkToB(bMeta)

			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			bRecord, err = dataProcessor.CreateRecord( "b", map[string]interface{}{"target": map[string]interface{}{"_object": "a", "id": aRecord.Data["id"]}, "name": "anotherbrecord"}, auth.User{})
			Expect(err).To(BeNil())

			_, matchedRecords, err := dataProcessor.GetBulk("a", fmt.Sprintf("eq(b_set.name,%s)", bRecord.Data["name"].(string)), nil, nil, 1, false)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
		})
	})
})
