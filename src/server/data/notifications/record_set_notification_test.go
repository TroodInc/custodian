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
	"strconv"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

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
		var aMetaObj *meta.Meta
		var aRecordData map[string]interface{}

		havingObjectA := func() {
			By("Having object A with action for 'create' defined")
			aMetaDescription := meta.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
					{
						Name:     "first_name",
						Type:     meta.FieldTypeString,
						Optional: false,
					},
					{
						Name:     "last_name",
						Type:     meta.FieldTypeString,
						Optional: false,
					},
				},
				Actions: []meta.Action{
					{
						Method:          meta.MethodCreate,
						Protocol:        meta.REST,
						Args:            []string{"http://example.com"},
						ActiveIfNotRoot: true,
						IncludeValues:   map[string]string{"last_name": "a_last_name"},
					},
				},
			}
			aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())
		}

		havingARecord := func() {
			By("Having a record of A object")
			aRecordData, err = dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"first_name": "Veronika", "last_name": "Petrova"}, auth.User{}, true)
			Expect(err).To(BeNil())
		}

		It("can capture previous record state on create", func() {

			havingObjectA()
			//start transaction
			executeContext, err := dataManager.NewExecuteContext()
			Expect(err).To(BeNil())
			defer executeContext.Close()

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				&record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"first_name": "Veronika", "last_name": "Petrova"}}},
				true,
				meta.MethodCreate,
				dataProcessor.GetBulk,
			)
			recordSetNotification.CapturePreviousState()
			//previous state for create operation should contain empty objects
			Expect(recordSetNotification.PreviousState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.PreviousState[0].DataSet[0]).To(HaveLen(0))
		})

		It("can capture current record state on create", func() {

			havingObjectA()

			recordSet := record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"first_name": "Veronika", "last_name": "Petrova"}}}

			dataProcessor.BeginTransaction()
			defer dataProcessor.CommitTransaction()

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				&recordSet,
				true,
				meta.MethodCreate,
				dataProcessor.GetBulk,
			)

			//assemble operations

			operation, err := dataManager.PrepareCreateOperation(recordSet.Meta, recordSet.DataSet)
			Expect(err).To(BeNil())

			// execute operation
			err = dataProcessor.ExecuteContext.Execute([]data.Operation{operation})
			Expect(err).To(BeNil())

			recordSet.CollapseLinks()

			recordSetNotification.CaptureCurrentState()

			//previous state for create operation should contain empty objects
			Expect(recordSetNotification.CurrentState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]).To(HaveLen(3))
		})

		It("can capture current state of existing record", func() {

			havingObjectA()
			havingARecord()

			recordSet := record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"id": aRecordData["id"], "last_name": "Kozlova"}}}

			dataProcessor.BeginTransaction()
			defer dataProcessor.CommitTransaction()

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				recordSet.Clone(),
				true,
				meta.MethodCreate,
				dataProcessor.GetBulk,
			)

			recordSetNotification.CaptureCurrentState()

			//only last_name specified for recordSet, thus first_name should not be included in notification message
			Expect(recordSetNotification.CurrentState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]).To(HaveLen(2))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]["a_last_name"].(string)).To(Equal("Petrova"))
		})

		It("can capture current state of existing record after update", func() {
			havingObjectA()
			havingARecord()

			recordSet := record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"id": aRecordData["id"], "last_name": "Ivanova"}}}

			dataProcessor.BeginTransaction()
			defer dataProcessor.CommitTransaction()

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				recordSet.Clone(),
				true,
				meta.MethodCreate,
				dataProcessor.GetBulk,
			)

			//assemble operations
			operation, err := dataManager.PrepareUpdateOperation(recordSet.Meta, recordSet.DataSet)
			Expect(err).To(BeNil())

			// execute operation
			err = dataProcessor.ExecuteContext.Execute([]data.Operation{operation})
			Expect(err).To(BeNil())

			recordSet.CollapseLinks()

			recordSetNotification.CaptureCurrentState()

			//only last_name specified for update, thus first_name should not be included in notification message
			Expect(recordSetNotification.CurrentState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]).To(HaveLen(2))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]["a_last_name"].(string)).To(Equal("Ivanova"))
		})

		It("can capture empty state of removed record after remove", func() {
			havingObjectA()
			havingARecord()

			recordSet := record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"id": aRecordData["id"], "last_name": "Ivanova"}}}

			dataProcessor.BeginTransaction()
			defer dataProcessor.CommitTransaction()

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				recordSet.Clone(),
				true,
				meta.MethodCreate,
				dataProcessor.GetBulk,
			)

			dataProcessor.DeleteRecord(aMetaObj.Name, strconv.Itoa(int(aRecordData["id"].(float64))), auth.User{}, true)

			recordSetNotification.CaptureCurrentState()

			//only last_name specified for update, thus first_name should not be included in notification message
			Expect(recordSetNotification.CurrentState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]).To(HaveLen(0))
		})
	})
})
