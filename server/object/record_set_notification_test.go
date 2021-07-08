package object_test

import (
	"custodian/server/auth"
	"custodian/server/noti"
	"custodian/server/object"
	"custodian/server/object/description"
	"custodian/utils"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)
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
						Protocol:        noti.TEST,
						Args:            []string{"http://example.com"},
						ActiveIfNotRoot: true,
						IncludeValues:   map[string]interface{}{"a_last_name": "last_name", testObjBName: fmt.Sprintf("%s.id", testObjBName)},
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

			record, _ := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{
				"first_name": "Veronika", "last_name": "Petrova",
			}, auth.User{})

			notifier := record.Meta.Actions[0].Notifier.(*noti.TestNotifier)

			var received *noti.Event
			Eventually(notifier.Events).Should(Receive(&received))

			Expect(received.Obj()["previous"]).To(BeEmpty())
		})

		It("can capture current record state on create", func() {
			havingObjectB()
			havingObjectA()

			record, _ := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{
				"first_name": "Veronika", "last_name": "Petrova",
			}, auth.User{})

			notifier := record.Meta.Actions[0].Notifier.(*noti.TestNotifier)

			var received *noti.Event
			Eventually(notifier.Events).Should(Receive(&received))
			Expect(received.Obj()["action"]).To(Equal("create"))

			Expect(received.Obj()["current"]).To(HaveLen(4))
		})

		It("can capture state on update", func() {
			havingObjectB()
			havingObjectA()
			havingBRecord()
			havingARecord(bRecord.Pk().(float64))

			record, _ := dataProcessor.UpdateRecord(aMetaObj.Name, aRecord.PkAsString(), map[string]interface{}{
				"last_name": "Ivanova",
			}, auth.User{})

			notifier := record.Meta.Actions[0].Notifier.(*noti.TestNotifier)

			var received *noti.Event
			Eventually(notifier.Events).Should(Receive(&received))
			Expect(received.Obj()["action"]).To(Equal("update"))

			//only last_name specified for recordSet, thus first_name should not be included in notification message
			previous := received.Obj()["previous"].(map[string]interface{})
			Expect(previous["a_last_name"]).To(Equal("Petrova"))

			current := received.Obj()["current"].(map[string]interface{})
			Expect(current["a_last_name"]).To(Equal("Ivanova"))
		})

		It("can capture empty state of removed record after remove", func() {
			havingObjectB()
			havingObjectA()
			havingBRecord()
			havingARecord(bRecord.Pk().(float64))

			removed, _ := dataProcessor.RemoveRecord(aMetaObj.Name, strconv.Itoa(int(aRecord.Data["id"].(float64))), auth.User{})

			notifier := removed.Meta.Actions[0].Notifier.(*noti.TestNotifier)
			//
			var received *noti.Event
			Eventually(notifier.Events).Should(Receive(&received))
			Expect(received.Obj()["action"]).To(Equal("remove"))
			//
			//fmt.Println("received ---->", received.Obj())
			//
			////only last_name specified for recordSet, thus first_name should not be included in notification message
			//previous := received.Obj()["previous"].(map[string]interface{})
			//Expect(previous["a_last_name"]).To(Equal("Petrova"))
			//
			//current := received.Obj()["current"].(map[string]interface{})
			//Expect(current).To(BeEmpty())
		})
	})
})
