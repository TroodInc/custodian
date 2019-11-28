package notifications_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"server/data"
	"utils"
	"server/auth"
	"server/object/meta"
	"server/object/description"
	. "server/data/notifications"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"strconv"
	"server/data/record"
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
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	Describe("RecordSetNotification state capturing", func() {

		var err error
		var aMetaObj *meta.Meta
		var bMetaObj *meta.Meta
		var cMetaObj *meta.Meta
		var aRecord *record.Record
		var bRecord *record.Record
		var cRecord *record.Record

		havingObjectA := func() {
			By("Having object A with action for 'create' defined")
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
						Name:         "target_object",
						LinkType:     description.LinkTypeInner,
						Type:         description.FieldTypeGeneric,
						LinkMetaList: []string{"b", "c"},
						Optional:     true,
					},
				},
				Actions: []description.Action{
					{
						Method:          description.MethodCreate,
						Protocol:        description.TEST,
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
						Name:     "first_name",
						Type:     description.FieldTypeString,
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
			cMetaDescription := description.MetaDescription{
				Name: "c",
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
			cMetaObj, err = metaStore.NewMeta(&cMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(cMetaObj)
			Expect(err).To(BeNil())
		}

		havingARecord := func(targetRecordObjectName string, targetRecordId float64) {
			By("Having a record of A object")
			aRecord, err = dataProcessor.CreateRecord(
				aMetaObj.Name,
				map[string]interface{}{"target_object": map[string]interface{}{"_object": targetRecordObjectName, "id": strconv.Itoa(int(targetRecordId))}},
				auth.User{},
			)
			Expect(err).To(BeNil())
		}

		havingBRecord := func() {
			By("Having a record of B object")
			bRecord, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"first_name": "Feodor"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingCRecord := func() {
			By("Having a record of C object")
			cRecord, err = dataProcessor.CreateRecord(cMetaObj.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("properly captures generic field value if action config does not match its object", func() {

			havingObjectB()
			havingObjectC()
			havingObjectA()
			havingBRecord()
			havingCRecord()
			havingARecord(cMetaObj.Name, cRecord.Pk().(float64))

			recordSet := record.RecordSet{Meta: aMetaObj, Records: []*record.Record{record.NewRecord(aMetaObj, map[string]interface{}{"id": aRecord.Pk()})}}

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				&recordSet,
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)

			recordSetNotification.CaptureCurrentState()

			//only last_name specified for recordSet, thus first_name should not be included in notification message
			Expect(recordSetNotification.CurrentState[0].Records).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].Records[0].Data).To(HaveLen(2))
			Expect(recordSetNotification.CurrentState[0].Records[0].Data["target_value"]).To(BeNil())
		})
	})
})
