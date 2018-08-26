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
		var cMetaObj *meta.Meta
		var aRecordData map[string]interface{}
		var bRecordData map[string]interface{}
		var cRecordData map[string]interface{}

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
					{
						Name:     "first_name",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
			}
			bMetaObj, err = metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, bMetaObj)
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
			err = metaStore.Create(globalTransaction, cMetaObj)
			Expect(err).To(BeNil())
		}

		havingARecord := func(targetRecordObjectName string, targetRecordId float64) {
			By("Having a record of A object")
			aRecordData, err = dataProcessor.CreateRecord(
				globalTransaction.DbTransaction,
				aMetaObj.Name,
				map[string]interface{}{
					"first_name":    "Veronika",
					"last_name":     "Petrova",
					"target_object": map[string]interface{}{"_object": targetRecordObjectName, "id": strconv.Itoa(int(targetRecordId))},
				},
				auth.User{},
			)
			Expect(err).To(BeNil())
		}

		havingBRecord := func() {
			By("Having a record of B object")
			bRecordData, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaObj.Name, map[string]interface{}{"first_name": "Feodor"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingCRecord := func() {
			By("Having a record of C object")
			cRecordData, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, cMetaObj.Name, map[string]interface{}{}, auth.User{})
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
			Expect(recordSetNotification.CurrentState[0].DataSet[0]).To(HaveLen(2))
			Expect(recordSetNotification.CurrentState[0].DataSet[0]["target_value"]).To(BeNil())
		})
	})
})
