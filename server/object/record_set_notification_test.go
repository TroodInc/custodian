package object_test

import (
	"custodian/server/auth"
	"custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/transactions"

	"custodian/utils"
	"fmt"
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)

	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)
	dataProcessor, _ := object.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	Describe("RecordSetNotification state capturing", func() {

		testObjAName := utils.RandomString(8)
		testObjBName := utils.RandomString(8)

		var err error
		var aMetaObj *object.Meta
		var bMetaObj *object.Meta
		var aRecord *object.Record
		var bRecord *object.Record

		havingObjectA := func() {
			By("Having object A with action for 'create' defined")
			aMetaDescription := description.MetaDescription{
				Name: testObjAName,
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
					{
						Name:     "last_name",
						Type:     description.FieldTypeString,
						Optional: false,
					},
					{
						Name:     testObjBName,
						LinkType: description.LinkTypeInner,
						Type:     description.FieldTypeObject,
						LinkMeta: testObjBName,
						Optional: true,
					},
				},
				Actions: []description.Action{
					{
						Method:          description.MethodCreate,
						Protocol:        description.TEST,
						Args:            []string{"http://example.com"},
						ActiveIfNotRoot: true,
						IncludeValues:   map[string]interface{}{"a_last_name": "last_name", testObjBName: fmt.Sprintf("%s.id", testObjBName)},
					},
					{
						Method:          description.MethodCreate,
						Protocol:        description.TEST,
						Args:            []string{"http://example1.com"},
						ActiveIfNotRoot: true,
						IncludeValues:   map[string]interface{}{},
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
				Name: testObjBName,
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
			bMetaObj, err = metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())
		}

		havingARecord := func(bRecordId float64) {
			By("Having a record of A object")
			aRecord, err = dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"first_name": "Veronika", "last_name": "Petrova", testObjBName: bRecordId}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingBRecord := func() {
			By("Having a record of B object")
			bRecord, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("can capture previous record state on create", func() {
			havingObjectB()
			havingObjectA()
			//make recordSetNotification
			recordSetNotification := object.NewRecordSetNotification(
				&object.RecordSet{Meta: aMetaObj, Records: []*object.Record{object.NewRecord(aMetaObj, map[string]interface{}{"first_name": "Veronika", "last_name": "Petrova"})}},
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)
			recordSetNotification.CapturePreviousState()

			//previous state for create operation should contain empty objects
			Expect(recordSetNotification.PreviousState[0].Records).To(HaveLen(1))
			Expect(recordSetNotification.PreviousState[0].Records[0]).To(BeNil())
		})

		It("can capture current record state on create", func() {
			havingObjectB()
			havingObjectA()
			recordSet := object.RecordSet{Meta: aMetaObj, Records: []*object.Record{object.NewRecord(aMetaObj, map[string]interface{}{"first_name": "Veronika", "last_name": "Petrova"})}}

			//make recordSetNotification
			recordSetNotification := object.NewRecordSetNotification(
				&recordSet,
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)

			//assemble operations
			operation, err := dataManager.PrepareCreateOperation(recordSet.Meta, recordSet.Data())
			Expect(err).To(BeNil())

			// execute operation
			globalTransaction, _ := dbTransactionManager.BeginTransaction()
			err = globalTransaction.Execute([]transactions.Operation{operation})
			Expect(err).To(BeNil())
			dbTransactionManager.CommitTransaction(globalTransaction)

			recordSet.CollapseLinks()

			recordSetNotification.CaptureCurrentState()

			//previous state for create operation should contain empty objects
			Expect(recordSetNotification.CurrentState[0].Records).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].Records[0].Data).To(HaveLen(4))
		})

		It("can capture current state of existing record", func() {

			havingObjectB()
			havingObjectA()
			havingBRecord()
			havingARecord(bRecord.Pk().(float64))

			recordSet := object.RecordSet{Meta: aMetaObj, Records: []*object.Record{object.NewRecord(aMetaObj, map[string]interface{}{"id": aRecord.Pk(), "last_name": "Kozlova"})}}

			//make recordSetNotification
			recordSetNotification := object.NewRecordSetNotification(
				&recordSet,
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)

			recordSetNotification.CaptureCurrentState()
			//only last_name specified for recordSet, thus first_name should not be included in notification message
			Expect(recordSetNotification.CurrentState[0].Records).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].Records[0].Data).To(HaveLen(3))
			Expect(recordSetNotification.CurrentState[0].Records[0].Data["a_last_name"].(string)).To(Equal("Petrova"))
		})

		It("can capture current state of existing record after update", func() {
			havingObjectB()
			havingObjectA()
			havingBRecord()
			havingARecord(bRecord.Pk().(float64))

			recordSet := object.RecordSet{Meta: aMetaObj, Records: []*object.Record{object.NewRecord(aMetaObj, map[string]interface{}{"id": aRecord.Pk(), "last_name": "Ivanova"})}}

			//make recordSetNotification

			recordSetNotification := object.NewRecordSetNotification(
				&recordSet,
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)

			//assemble operations
			operation, err := dataManager.PrepareUpdateOperation(recordSet.Meta, recordSet.Data())
			Expect(err).To(BeNil())

			// execute operation
			globalTransaction, _ := dbTransactionManager.BeginTransaction()
			err = globalTransaction.Execute([]transactions.Operation{operation})
			Expect(err).To(BeNil())
			dbTransactionManager.CommitTransaction(globalTransaction)

			recordSet.CollapseLinks()

			recordSetNotification.CaptureCurrentState()
			//only last_name specified for update, thus first_name should not be included in notification message
			Expect(recordSetNotification.CurrentState[0].Records).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].Records[0].Data).To(HaveLen(4))
			Expect(recordSetNotification.CurrentState[0].Records[0].Data["a_last_name"].(string)).To(Equal("Ivanova"))
		})

		It("can capture empty state of removed record after remove", func() {
			havingObjectB()
			havingObjectA()
			havingBRecord()
			havingARecord(bRecord.Pk().(float64))

			recordSet := object.RecordSet{Meta: aMetaObj, Records: []*object.Record{object.NewRecord(aMetaObj, map[string]interface{}{"id": aRecord.Pk(), "last_name": "Ivanova"})}}

			//make recordSetNotification
			recordSetNotification := object.NewRecordSetNotification(
				&recordSet,
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)

			dataProcessor.RemoveRecord(aMetaObj.Name, strconv.Itoa(int(aRecord.Data["id"].(float64))), auth.User{})

			recordSetNotification.CaptureCurrentState()
			//only last_name specified for update, thus first_name should not be included in notification message
			Expect(recordSetNotification.CurrentState[0].Records).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].Records[0]).To(BeNil())
		})

		It("forms correct notification message on record removal", func() {
			havingObjectB()
			havingObjectA()
			havingBRecord()
			havingARecord(bRecord.Pk().(float64))

			recordSet := object.RecordSet{Meta: aMetaObj, Records: []*object.Record{object.NewRecord(aMetaObj, map[string]interface{}{"id": aRecord.Pk(), "last_name": "Ivanova"})}}

			//make recordSetNotification
			recordSetNotification := object.NewRecordSetNotification(
				&recordSet,
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)
			recordSetNotification.CapturePreviousState()

			dataProcessor.RemoveRecord(aMetaObj.Name, strconv.Itoa(int(aRecord.Pk().(float64))), auth.User{})
			recordSetNotification.CaptureCurrentState()
			notificationsData := recordSetNotification.BuildNotificationsData(
				recordSetNotification.PreviousState[0],
				recordSetNotification.CurrentState[0],
				auth.User{},
			)
			Expect(notificationsData).To(HaveLen(1))
			Expect(notificationsData[0]).To(HaveKey("previous"))
			Expect(notificationsData[0]).To(HaveKey("current"))
		})
	})
})
