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
	"server/data/record"
	"strconv"
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

	Describe("RecordSetNotification state capturing", func() {

		var err error
		var aMetaObj *meta.Meta
		var bMetaObj *meta.Meta
		var aRecordData map[string]interface{}
		var bRecordData map[string]interface{}

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
						Name:     "b",
						LinkType: description.LinkTypeInner,
						Type:     description.FieldTypeObject,
						LinkMeta: "b",
						Optional: true,
					},
				},
				Actions: []description.Action{
					{
						Method:          description.MethodCreate,
						Protocol:        description.TEST,
						Args:            []string{"http://example.com"},
						ActiveIfNotRoot: true,
						IncludeValues:   map[string]interface{}{"a_last_name": "last_name", "b": "b.id"},
					},
				},
			}
			aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, aMetaObj)
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
				},
			}
			bMetaObj, err = metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, bMetaObj)
			Expect(err).To(BeNil())
		}

		havingARecord := func(bRecordId float64) {
			By("Having a record of A object")
			aRecordData, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaObj.Name, map[string]interface{}{"first_name": "Veronika", "last_name": "Petrova", "b": bRecordId}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingBRecord := func() {
			By("Having a record of B object")
			bRecordData, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaObj.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("can capture previous record state on create", func() {
			havingObjectB()
			havingObjectA()
			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				globalTransaction.DbTransaction,
				&record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"first_name": "Veronika", "last_name": "Petrova"}}},
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)
			recordSetNotification.CapturePreviousState()
			//previous state for create operation should contain empty objects
			Expect(recordSetNotification.PreviousState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.PreviousState[0].DataSet[0]).To(HaveLen(0))
		})

		It("can capture current record state on create", func() {

			havingObjectB()
			havingObjectA()

			recordSet := record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"first_name": "Veronika", "last_name": "Petrova"}}}

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				globalTransaction.DbTransaction,
				&recordSet,
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)

			//assemble operations

			operation, err := dataManager.PrepareCreateOperation(recordSet.Meta, recordSet.DataSet)
			Expect(err).To(BeNil())

			// execute operation
			err = globalTransaction.DbTransaction.Execute([]transactions.Operation{operation})
			Expect(err).To(BeNil())

			recordSet.CollapseLinks()

			recordSetNotification.CaptureCurrentState()

			//previous state for create operation should contain empty objects
			Expect(recordSetNotification.CurrentState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]).To(HaveLen(4))
		})

		It("can capture current state of existing record", func() {

			havingObjectB()
			havingObjectA()
			havingBRecord()
			havingARecord(bRecordData["id"].(float64))

			recordSet := record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"id": aRecordData["id"], "last_name": "Kozlova"}}}

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				globalTransaction.DbTransaction,
				&recordSet,
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)

			recordSetNotification.CaptureCurrentState()

			//only last_name specified for recordSet, thus first_name should not be included in notification message
			Expect(recordSetNotification.CurrentState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]).To(HaveLen(3))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]["a_last_name"].(string)).To(Equal("Petrova"))
		})

		It("can capture current state of existing record after update", func() {
			havingObjectB()
			havingObjectA()
			havingBRecord()
			havingARecord(bRecordData["id"].(float64))

			recordSet := record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"id": aRecordData["id"], "last_name": "Ivanova"}}}

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				globalTransaction.DbTransaction,
				&recordSet,
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)

			//assemble operations
			operation, err := dataManager.PrepareUpdateOperation(recordSet.Meta, recordSet.DataSet)
			Expect(err).To(BeNil())

			// execute operation
			err = globalTransaction.DbTransaction.Execute([]transactions.Operation{operation})
			Expect(err).To(BeNil())

			recordSet.CollapseLinks()

			recordSetNotification.CaptureCurrentState()

			//only last_name specified for update, thus first_name should not be included in notification message
			Expect(recordSetNotification.CurrentState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]).To(HaveLen(4))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]["a_last_name"].(string)).To(Equal("Ivanova"))
		})

		It("can capture empty state of removed record after remove", func() {
			havingObjectB()
			havingObjectA()
			havingBRecord()
			havingARecord(bRecordData["id"].(float64))

			recordSet := record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"id": aRecordData["id"], "last_name": "Ivanova"}}}

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				globalTransaction.DbTransaction,
				&recordSet,
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)

			dataProcessor.RemoveRecord(globalTransaction.DbTransaction, aMetaObj.Name, strconv.Itoa(int(aRecordData["id"].(float64))), auth.User{})

			recordSetNotification.CaptureCurrentState()

			//only last_name specified for update, thus first_name should not be included in notification message
			Expect(recordSetNotification.CurrentState[0].DataSet).To(HaveLen(1))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]).To(HaveLen(0))
		})

		It("forms correct notification message on record removal", func() {
			havingObjectB()
			havingObjectA()
			havingBRecord()
			havingARecord(bRecordData["id"].(float64))

			recordSet := record.RecordSet{Meta: aMetaObj, DataSet: []map[string]interface{}{{"id": aRecordData["id"], "last_name": "Ivanova"}}}

			//make recordSetNotification
			recordSetNotification := NewRecordSetNotification(
				globalTransaction.DbTransaction,
				&recordSet,
				true,
				description.MethodCreate,
				dataProcessor.GetBulk,
				dataProcessor.Get,
			)
			recordSetNotification.CapturePreviousState()

			dataProcessor.RemoveRecord(globalTransaction.DbTransaction, aMetaObj.Name, strconv.Itoa(int(aRecordData["id"].(float64))), auth.User{})
			recordSetNotification.CaptureCurrentState()
			notificationsData := recordSetNotification.BuildNotificationsData(0, auth.User{})
			Expect(notificationsData).To(HaveLen(1))
			Expect(notificationsData[0]).To(HaveKey("previous"))
			Expect(notificationsData[0]).To(HaveKey("current"))
		})
	})
})
