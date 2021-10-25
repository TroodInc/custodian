package object_test

import (
	"custodian/server/auth"
	migrations_description "custodian/server/migrations/description"
	"custodian/server/noti"
	"custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/migrations/managers"
	"custodian/utils"
	"fmt"
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Data 101", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache(), db)
	migrationManager := managers.NewMigrationManager(
		metaDescriptionSyncer, dbTransactionManager, db,
	)
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
		var aRecord *object.Record
		var bRecord *object.Record
		aFields := []description.Field{
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
		}

		havingObjectA := func(actions []description.Action) {
			aMetaDescription := description.NewMetaDescription(testObjAName, "id", aFields, actions, false)
			migrationDescription := &migrations_description.MigrationDescription{
				Id:        utils.RandomString(8),
				ApplyTo:   "",
				DependsOn: nil,
				Operations: []migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: aMetaDescription,
					},
				},
			}

			_, err := migrationManager.Apply(migrationDescription, true, false)
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

			migrationDescription := &migrations_description.MigrationDescription{
				Id:        utils.RandomString(8),
				ApplyTo:   "",
				DependsOn: nil,
				Operations: []migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: &bMetaDescription,
					},
				},
			}

			_, err := migrationManager.Apply(migrationDescription, true, false)
			Expect(err).To(BeNil())
		}

		havingARecord := func(bRecordId float64) {
			By("Having a record of A object")
			aRecord, err = dataProcessor.CreateRecord(testObjAName, map[string]interface{}{"first_name": "Veronika", "last_name": "Petrova", testObjBName: bRecordId}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingBRecord := func() {
			By("Having a record of B object")
			bRecord, err = dataProcessor.CreateRecord(testObjBName, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("can capture previous record state on create", func() {
			crateAction := []description.Action{
				{
					Method:          description.MethodCreate,
					Protocol:        noti.TEST,
					Args:            []string{"http://example.com"},
					ActiveIfNotRoot: true,
					IncludeValues:   map[string]interface{}{"a_last_name": "last_name", testObjBName: fmt.Sprintf("%s.id", testObjBName)},
				},
			}

			havingObjectB()
			havingObjectA(crateAction)

			record, _ := dataProcessor.CreateRecord(testObjAName, map[string]interface{}{
				"first_name": "Veronika", "last_name": "Petrova",
			}, auth.User{})

			notifier := record.Meta.Actions[0].Notifier.(*noti.TestNotifier)

			var received *noti.Event
			Eventually(notifier.Events).Should(Receive(&received))

			Expect(received.Obj()["previous"]).To(BeEmpty())
		})

		It("can capture current record state on create", func() {
			createAction := []description.Action{
				{
					Method:          description.MethodCreate,
					Protocol:        noti.TEST,
					Args:            []string{"http://example.com"},
					ActiveIfNotRoot: true,
					IncludeValues:   map[string]interface{}{"a_last_name": "last_name", testObjBName: fmt.Sprintf("%s.id", testObjBName)},
				},
			}

			havingObjectB()
			havingObjectA(createAction)

			record, _ := dataProcessor.CreateRecord(testObjAName, map[string]interface{}{
				"first_name": "Veronika", "last_name": "Petrova",
			}, auth.User{})

			notifier := record.Meta.Actions[0].Notifier.(*noti.TestNotifier)

			var received *noti.Event
			Eventually(notifier.Events).Should(Receive(&received))
			Expect(received.Obj()["action"]).To(Equal("create"))

			Expect(received.Obj()["current"]).To(HaveLen(4))
		})

		It("can capture state on update", func() {
			updateAction := []description.Action{
				{
					Method:          description.MethodUpdate,
					Protocol:        noti.TEST,
					Args:            []string{"http://example.com"},
					ActiveIfNotRoot: true,
					IncludeValues:   map[string]interface{}{"a_last_name": "last_name", testObjBName: fmt.Sprintf("%s.id", testObjBName)},
				},
			}

			havingObjectB()
			havingObjectA(updateAction)
			havingBRecord()
			havingARecord(bRecord.Pk().(float64))

			record, _ := dataProcessor.UpdateRecord(testObjAName, aRecord.PkAsString(), map[string]interface{}{
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

		It("can capture state two actions", func() {
			actions := []description.Action{
				{
					Name:            "Update Action 1",
					Method:          description.MethodUpdate,
					Protocol:        noti.TEST,
					Args:            []string{"http://example.com"},
					ActiveIfNotRoot: true,
				},
				{
					Name:            "Update Action 2",
					Method:          description.MethodUpdate,
					Protocol:        noti.TEST,
					Args:            []string{"http://example.com"},
					ActiveIfNotRoot: true,
				},
				{
					Name:            "Delete Action 1",
					Method:          description.MethodRemove,
					Protocol:        noti.TEST,
					Args:            []string{"http://example.com"},
					ActiveIfNotRoot: true,
				},
				{
					Name:            "Delete Action 2",
					Method:          description.MethodRemove,
					Protocol:        noti.TEST,
					Args:            []string{"http://example.com"},
					ActiveIfNotRoot: true,
				},
			}

			havingObjectB()
			havingObjectA(actions)
			havingBRecord()
			havingARecord(bRecord.Pk().(float64))

			record, _ := dataProcessor.UpdateRecord(testObjAName, aRecord.PkAsString(), map[string]interface{}{
				"last_name": "Ivanova",
			}, auth.User{})

			for _, action := range record.Meta.Actions {
				notifier := action.Notifier.(*noti.TestNotifier)
				if action.Method == description.MethodUpdate {
					Consistently(notifier.Events).Should(HaveLen(1))
					event := <-notifier.Events
					Expect(event.Obj()["action"]).To(Equal("update"))
				} else {
					Consistently(notifier.Events).Should(HaveLen(0))
				}
			}

		})

		It("can capture empty state of removed record after remove", func() {
			removeAction := []description.Action{
				{
					Method:          description.MethodRemove,
					Protocol:        noti.TEST,
					Args:            []string{"http://example.com"},
					ActiveIfNotRoot: true,
					IncludeValues:   map[string]interface{}{"a_last_name": "last_name", testObjBName: fmt.Sprintf("%s.id", testObjBName)},
				},
			}
			havingObjectB()
			havingObjectA(removeAction)
			havingBRecord()
			havingARecord(bRecord.Pk().(float64))

			removed, _ := dataProcessor.RemoveRecord(testObjAName, strconv.Itoa(int(aRecord.Data["id"].(float64))), auth.User{})

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
