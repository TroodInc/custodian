package notifications_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/auth"
	"server/data"
	. "server/data/notifications"
	"server/noti"
	"server/object"
	"server/object/driver"
	"server/object/meta"
	"server/pg"
	"server/pg/transactions"
	"time"
	"utils"
)

var _ = XDescribe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	dbTransactionManager := transactions.NewPgDbTransactionManager(dataManager)

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := object.NewStore(driver)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		metaStore.Flush()
	})

	GetMetaA := func() *meta.Meta {
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		aMetaDescription.AddField(&meta.Field{Name:"name", Type: meta.FieldTypeString, Optional: false})
		aMetaDescription.Actions = []*Action{
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
		}
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		return metaStore.Create(aMetaObj)
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