package notifications_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/auth"
	"server/data"
	. "server/data/notifications"
	"server/noti"
	"server/object"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"time"
	"utils"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := object.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	GetMetaA := func() *object.Meta {
		aMetaDescription := object.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "name",
					Type:     object.FieldTypeString,
					Optional: false,
				},
			},
			Actions: []*Action{
				{
					Method:          MethodCreate,
					Protocol:        noti.TEST,
					Args:            []string{"http://example.com"},
					ActiveIfNotRoot: true,
					IncludeValues:   map[string]interface{}{},
				},
				{
					Method:          MethodUpdate,
					Protocol:        noti.TEST,
					Args:            []string{"http://example.com"},
					ActiveIfNotRoot: true,
					IncludeValues:   map[string]interface{}{},
				},
				{
					Method:          MethodRemove,
					Protocol:        noti.TEST,
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

	var TNotifier noti.Notifier
	BeforeEach(func() {
		TNotifier, _ = noti.NewTestNotifier(nil, true)

		dataProcessor.RecordSetNotificationPool = &RecordSetNotificationPool{
			Notifiers: map[string]map[Method][] *noti.Notifier{
				"a": {
					MethodCreate: []*noti.Notifier{ &TNotifier },
					MethodUpdate: []*noti.Notifier{ &TNotifier },
					MethodRemove: []*noti.Notifier{ &TNotifier },
				},
			},
		}

	})

	It("Should send notification on Create event", func() {
		aMetaObj := GetMetaA()

		record, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "Alex"}, auth.User{})
		Expect(err).To(BeNil())

		// TODO: Fix flaky test
		time.Sleep(2000)

		Eventually(TNotifier.(*noti.TestNotifier).Buff).Should(
			ContainElement(
				SatisfyAll(
					HaveKeyWithValue("current", record.Data),
					HaveKeyWithValue("action", "create"),
					HaveKeyWithValue("object", record.Meta.Name),
				),
			),
		)
	})

	It("Should send notification on Update event", func() {
		aMetaObj := GetMetaA()

		initialRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "Rob"}, auth.User{})
		Expect(err).To(BeNil())

		updatedRecord, err := dataProcessor.UpdateRecord(aMetaObj.Name, initialRecord.PkAsString(), map[string]interface{}{"name": "Jane"}, auth.User{})
		Expect(err).To(BeNil())

		// TODO: Fix flaky test
		time.Sleep(2000)

		Eventually(TNotifier.(*noti.TestNotifier).Buff).Should(
			ContainElement(
				SatisfyAll(
					HaveKeyWithValue("current", updatedRecord.Data),
					HaveKeyWithValue("action", "update"),
					HaveKeyWithValue("object", initialRecord.Meta.Name),
				),
			),
		)
	})

	It("Should send notification on Delete event", func() {
		aMetaObj := GetMetaA()

		initialRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "Fox"}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.RemoveRecord(aMetaObj.Name, initialRecord.PkAsString(), auth.User{})
		Expect(err).To(BeNil())

		// TODO: Fix flaky test
		time.Sleep(2000)

		Eventually(TNotifier.(*noti.TestNotifier).Buff).Should(
			ContainElement(
				SatisfyAll(
					HaveKeyWithValue("previous", initialRecord.Data),
					HaveKeyWithValue("action", "remove"),
					HaveKeyWithValue("object", initialRecord.Meta.Name),
				),
			),
		)
	})

})