package object_test

import (
	"custodian/server/auth"
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	dataManager := object.DBManager{}
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)
	dataProcessor, _ := object.NewProcessor(metaStore, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	Describe("RecordSetNotification state capturing", func() {

		testObjAName := utils.RandomString(8)
		testObjBName := utils.RandomString(8)

		havingObjectA := func(onDeleteStrategy description.OnDeleteStrategy) *object.Meta {
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
						Name:     testObjBName,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjBName,
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
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())
			return aMetaObj
		}

		havingObjectB := func() *object.Meta {
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
			bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())
			return bMetaObj
		}

		havingARecord := func(bRecordId float64) *object.Record {
			aMetaObj := havingObjectA(description.OnDeleteCascade)
			By("Having a record of A object")
			aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{testObjBName: bRecordId}, auth.User{})
			Expect(err).To(BeNil())
			return aRecord
		}

		havingBRecord := func() *object.Record {
			bMetaObj := havingObjectB()
			By("Having a record of B object")
			bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())
			return bRecord
		}

		It("makes correct notification messages on record removal with `cascade` remove", func() {

			bRecord := havingBRecord()
			Expect(bRecord).NotTo(BeNil())
			aRecord := havingARecord(bRecord.Pk().(float64))

			recordSetNotificationPool := object.NewRecordSetNotificationPool()

			bRecord, err := dataProcessor.Get(bRecord.Meta.Name, bRecord.PkAsString(), nil, nil, 1, false)
			Expect(err).To(BeNil())

			//fill node
			globalTransaction, _ := dbTransactionManager.BeginTransaction()
			removalRootNode, err := new(object.RecordRemovalTreeBuilder).Extract(bRecord, dataProcessor, globalTransaction)
			Expect(err).To(BeNil())

			err = dataManager.PerformRemove(removalRootNode, globalTransaction, recordSetNotificationPool, dataProcessor)
			Expect(err).To(BeNil())
			dbTransactionManager.CommitTransaction(globalTransaction)

			notifications := recordSetNotificationPool.Notifications()

			Expect(notifications).To(HaveLen(2))

			Expect(notifications[0].CurrentState).To(HaveLen(1))
			Expect(notifications[0].Method).To(Equal(description.MethodRemove))
			Expect(notifications[0].CurrentState[0].Meta.Name).To(Equal(aRecord.Meta.Name))
			Expect(notifications[0].CurrentState[0].Records[0]).To(BeNil())
			Expect(notifications[0].PreviousState[0].Records[0].Data["id"]).To(Equal(aRecord.Pk()))

			Expect(notifications[1].CurrentState).To(HaveLen(1))
			Expect(notifications[1].Method).To(Equal(description.MethodRemove))
			Expect(notifications[1].CurrentState[0].Meta.Name).To(Equal(bRecord.Meta.Name))
			Expect(notifications[1].CurrentState[0].Records[0]).To(BeNil())
			Expect(notifications[1].PreviousState[0].Records[0].Data["id"]).To(Equal(bRecord.Pk()))
		})

		XIt("makes correct notification messages on record removal with `setNull` remove", func() {
			bRecord := havingBRecord()
			aMetaObj := havingObjectA(description.OnDeleteSetNull)
			dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{testObjBName: bRecord.Pk().(float64)}, auth.User{})

			recordSetNotificationPool := object.NewRecordSetNotificationPool()

			//fill node
			globalTransaction, _ := dbTransactionManager.BeginTransaction()
			removalRootNode, err := new(object.RecordRemovalTreeBuilder).Extract(bRecord, dataProcessor, globalTransaction)
			Expect(err).To(BeNil())

			err = dataManager.PerformRemove(removalRootNode, globalTransaction, recordSetNotificationPool, dataProcessor)
			Expect(err).To(BeNil())
			dbTransactionManager.CommitTransaction(globalTransaction)

			dataProcessor.RemoveRecord(bRecord.Meta.Name, strconv.Itoa(int(bRecord.Pk().(float64))), auth.User{})
			notifications := recordSetNotificationPool.Notifications()

			Expect(notifications).To(HaveLen(2))

			Expect(notifications[0].CurrentState).To(HaveLen(1))
			Expect(notifications[0].Method).To(Equal(description.MethodUpdate))
			//Expect(notifications[0].CurrentState[1].Meta.Name).To(Equal(aMetaObj.Name))
			Expect(notifications[0].CurrentState).To(HaveLen(1))
			Expect(notifications[0].CurrentState[1].Records[0]).To(Not(BeNil()))
			//Expect(notifications[0].PreviousState[1].Records[0].Data["id"]).To(Equal(aRecord.Pk()))

			Expect(notifications[1].CurrentState).To(HaveLen(1))
			Expect(notifications[1].Method).To(Equal(description.MethodRemove))
			Expect(notifications[1].CurrentState[0].Meta.Name).To(Equal(bRecord.Meta.Name))
			Expect(notifications[1].CurrentState).To(HaveLen(1))
			Expect(notifications[1].CurrentState[0].Records[0]).To(BeNil())
			Expect(notifications[1].PreviousState[0].Records[0].Data["id"]).To(Equal(bRecord.Pk()))
		})
	})
})
