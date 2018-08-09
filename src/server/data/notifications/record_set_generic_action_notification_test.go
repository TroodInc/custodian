package notifications_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
	"server/pg"
	"server/data"
	"utils"
	"server/data/record"
	. "server/data/notifications"
	"server/auth"
	//"strconv"
	"strconv"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := object.NewStore(object.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	Describe("RecordSetNotification state capturing", func() {

		var err error
		var aMetaObj *object.Meta
		var bMetaObj *object.Meta
		var cMetaObj *object.Meta
		var aRecordData map[string]interface{}
		var bRecordData map[string]interface{}
		var cRecordData map[string]interface{}

		havingObjectA := func() {
			By("Having object A with action for 'create' defined")
			aMetaDescription := object.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name: "id",
						Type: object.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
					{
						Name:         "target_object",
						LinkType:     object.LinkTypeInner,
						Type:         object.FieldTypeGeneric,
						LinkMetaList: []string{"b", "c"},
						Optional:     true,
					},
				},
				Actions: []object.Action{
					{
						Method:          object.MethodCreate,
						Protocol:        object.TEST,
						Args:            []string{"http://example.com"},
						ActiveIfNotRoot: true,
						IncludeValues: map[string]interface{}{"target_value": map[string]interface{}{
							"field": "target_object",
							"cases": []interface{}{
								map[string]interface{}{
									"object": "b",
									"value":  "first_name",
								},
							},
						}},
					},
				},
			}
			aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())
		}

		havingObjectB := func() {
			By("Having object B which")
			bMetaDescription := object.MetaDescription{
				Name: "b",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name: "id",
						Type: object.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
					{
						Name:     "first_name",
						Type:     object.FieldTypeString,
						Optional: false,
					},
				},
			}
			bMetaObj, err = metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())
		}

		havingObjectC := func() {
			By("Having object C")
			cMetaDescription := object.MetaDescription{
				Name: "c",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name: "id",
						Type: object.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
				},
			}
			cMetaObj, err = metaStore.NewMeta(&cMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(cMetaObj)
			Expect(err).To(BeNil())
		}

		havingARecord := func(targetRecordObjectName string, targetRecordId float64) {
			By("Having a record of A object")
			aRecordData, err = dataProcessor.CreateRecord(
				aMetaObj.Name,
				map[string]interface{}{
					"first_name":    "Veronika",
					"last_name":     "Petrova",
					"target_object": map[string]interface{}{"_object": targetRecordObjectName, "id": strconv.Itoa(int(targetRecordId))},
				},
				auth.User{},
				true,
			)
			Expect(err).To(BeNil())
		}

		havingBRecord := func() {
			By("Having a record of B object")
			bRecordData, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"first_name": "Feodor"}, auth.User{}, true)
			Expect(err).To(BeNil())
		}

		havingCRecord := func() {
			By("Having a record of C object")
			cRecordData, err = dataProcessor.CreateRecord(cMetaObj.Name, map[string]interface{}{}, auth.User{}, true)
			Expect(err).To(BeNil())
		}

		It("propery captures generic field value if action config does not match its object", func() {

			havingObjectB()
			havingObjectC()
			havingObjectA()
			havingBRecord()
			havingCRecord()
			havingARecord(cMetaObj.Name, cRecordData["id"].(float64))

			recordSet := record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"id": aRecordData["id"]}}}

			dataProcessor.BeginTransaction()
			defer dataProcessor.CommitTransaction()

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				&recordSet,
				true,
				object.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)

			recordSetNotification.CaptureCurrentState()

			//only last_name specified for recordSet, thus first_name should not be included in notification message
			Expect(recordSetNotification.CurrentState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]).To(HaveLen(2))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]["target_value"]).To(BeNil())
		})
	})
})
