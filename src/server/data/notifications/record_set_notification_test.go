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

		//var aRecord map[string]interface{}
		var err error
		var aMetaObj *meta.Meta

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
			var operations = make([]data.Operation, 0)
			operation, err := dataManager.PreparePuts(recordSet.Meta, recordSet.DataSet)
			Expect(err).To(BeNil())
			operations = append(operations, operation)

			// execute operation
			err = dataProcessor.ExecuteContext.Execute(operations)
			Expect(err).To(BeNil())

			recordSet.CollapseLinks()

			recordSetNotification.CaptureCurrentState()

			//previous state for create operation should contain empty objects
			Expect(recordSetNotification.CurrentState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]).To(HaveLen(3))
		})
	})
})
