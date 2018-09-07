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

		havingObjectA := func(onDeleteStrategy description.OnDeleteStrategy) {
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
						Name:     "b",
						LinkType: description.LinkTypeInner,
						Type:     description.FieldTypeObject,
						LinkMeta: "b",
						OnDelete: onDeleteStrategy.ToVerbose(),
						Optional: true,
					},
				},
				Actions: []description.Action{
					{
						Method:          description.MethodRemove,
						Protocol:        description.TEST,
						Args:            []string{"http://example.com"},
						ActiveIfNotRoot: true,
						IncludeValues:   map[string]interface{}{},
					},
					{
						Method:          description.MethodUpdate,
						Protocol:        description.TEST,
						Args:            []string{"http://example.com"},
						ActiveIfNotRoot: true,
						IncludeValues:   map[string]interface{}{},
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
				Actions: []description.Action{
					{
						Method:          description.MethodRemove,
						Protocol:        description.TEST,
						Args:            []string{"http://example.com"},
						ActiveIfNotRoot: true,
						IncludeValues:   map[string]interface{}{},
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
			aRecordData, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaObj.Name, map[string]interface{}{"b": bRecordId}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingBRecord := func() {
			By("Having a record of B object")
			bRecordData, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaObj.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("makes correct notification messages on record removal with `cascade` remove", func() {
			havingObjectB()
			havingObjectA(description.OnDeleteCascade)
			havingBRecord()
			havingARecord(bRecordData["id"].(float64))

			recordSetNotificationPool := NewRecordSetNotificationPool()

			bPkey, _ := bMetaObj.Key.ValueAsString(bRecordData["id"])
			bRecord, err := dataProcessor.Get(globalTransaction.DbTransaction, bMetaObj.Name, bPkey, 1)
			Expect(err).To(BeNil())

			//fill node
			removalRootNode, err := new(data.RecordRemovalTreeExtractor).Extract(bRecord, dataProcessor, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			err = dataManager.PerformRemove(removalRootNode, globalTransaction.DbTransaction, recordSetNotificationPool, dataProcessor)
			Expect(err).To(BeNil())

			notifications := recordSetNotificationPool.Notifications()

			Expect(notifications).To(HaveLen(2))

			Expect(notifications[0].CurrentState).To(HaveLen(1))
			Expect(notifications[0].Method).To(Equal(description.MethodRemove))
			Expect(notifications[0].CurrentState[0].Meta.Name).To(Equal(aMetaObj.Name))
			Expect(notifications[0].CurrentState[0].DataSet[0]).To(BeNil())
			Expect(notifications[0].PreviousState[0].DataSet[0]["id"]).To(Equal(aRecordData["id"]))

			Expect(notifications[1].CurrentState).To(HaveLen(1))
			Expect(notifications[1].Method).To(Equal(description.MethodRemove))
			Expect(notifications[1].CurrentState[0].Meta.Name).To(Equal(bMetaObj.Name))
			Expect(notifications[1].CurrentState[0].DataSet[0]).To(BeNil())
			Expect(notifications[1].PreviousState[0].DataSet[0]["id"]).To(Equal(bRecordData["id"]))
		})

		It("makes correct notification messages on record removal with `setNull` remove", func() {
			havingObjectB()
			havingObjectA(description.OnDeleteSetNull)
			havingBRecord()
			havingARecord(bRecordData["id"].(float64))

			recordSetNotificationPool := NewRecordSetNotificationPool()

			bPkey, _ := bMetaObj.Key.ValueAsString(bRecordData["id"])
			bRecord, err := dataProcessor.Get(globalTransaction.DbTransaction, bMetaObj.Name, bPkey, 1)
			Expect(err).To(BeNil())

			//fill node
			removalRootNode, err := new(data.RecordRemovalTreeExtractor).Extract(bRecord, dataProcessor, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			err = dataManager.PerformRemove(removalRootNode, globalTransaction.DbTransaction, recordSetNotificationPool, dataProcessor)
			Expect(err).To(BeNil())

			dataProcessor.RemoveRecord(globalTransaction.DbTransaction, bMetaObj.Name, strconv.Itoa(int(bRecordData["id"].(float64))), auth.User{})
			notifications := recordSetNotificationPool.Notifications()

			Expect(notifications).To(HaveLen(2))

			Expect(notifications[0].CurrentState).To(HaveLen(1))
			Expect(notifications[0].Method).To(Equal(description.MethodUpdate))
			Expect(notifications[0].CurrentState[1].Meta.Name).To(Equal(aMetaObj.Name))
			Expect(notifications[0].CurrentState).To(HaveLen(1))
			Expect(notifications[0].CurrentState[1].DataSet[0]).To(Not(BeNil()))
			Expect(notifications[0].PreviousState[1].DataSet[0]["id"]).To(Equal(aRecordData["id"]))

			Expect(notifications[1].CurrentState).To(HaveLen(1))
			Expect(notifications[1].Method).To(Equal(description.MethodRemove))
			Expect(notifications[1].CurrentState[0].Meta.Name).To(Equal(bMetaObj.Name))
			Expect(notifications[1].CurrentState).To(HaveLen(1))
			Expect(notifications[1].CurrentState[0].DataSet[0]).To(BeNil())
			Expect(notifications[1].PreviousState[0].DataSet[0]["id"]).To(Equal(bRecordData["id"]))
		})
	})
})
