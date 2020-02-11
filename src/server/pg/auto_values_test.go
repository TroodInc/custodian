package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/auth"
	"server/data"
	"server/object"
	"server/pg"
	"utils"

	pg_transactions "server/pg/transactions"
	"server/transactions"
)

var _ = Describe("PG Auto Values Test", func() {
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

	Context("Having an object with fields with autoOnUpdate set to true", func() {
		var metaObj *object.Meta

		BeforeEach(func() {
			var err error
			metaDescription := object.Meta{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:        "datetime",
						Type:        object.FieldTypeDateTime,
						NowOnUpdate: true,
						Optional:    true,
					},
					{
						Name:        "date",
						Type:        object.FieldTypeDate,
						NowOnUpdate: true,
						Optional:    true,
					},
					{
						Name:        "time",
						Type:        object.FieldTypeTime,
						NowOnUpdate: true,
						Optional:    true,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())
		})

		It("can set auto values", func() {
			record, err := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 1}, auth.User{})
			Expect(err).To(BeNil())

			Expect(record.Data["datetime"]).To(BeNil())
			Expect(record.Data["date"]).To(BeNil())
			Expect(record.Data["time"]).To(BeNil())

			record, err = dataProcessor.UpdateRecord(metaObj.Name, record.PkAsString(), record.Data, auth.User{})

			Expect(record.Data["datetime"]).NotTo(BeNil())
			Expect(record.Data["date"]).NotTo(BeNil())
			Expect(record.Data["time"]).NotTo(BeNil())
		})
	})

	Context("Having an object with fields with autoOnCreate set to true", func() {
		var metaObj *object.Meta

		BeforeEach(func() {
			var err error
			metaDescription := object.Meta{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:        "datetime",
						Type:        object.FieldTypeDateTime,
						NowOnCreate: true,
						Optional:    true,
					},
					{
						Name:        "date",
						Type:        object.FieldTypeDate,
						NowOnCreate: true,
						Optional:    true,
					},
					{
						Name:        "time",
						Type:        object.FieldTypeTime,
						NowOnCreate: true,
						Optional:    true,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())
		})

		It("can set auto values", func() {
			record, err := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 1}, auth.User{})
			Expect(err).To(BeNil())

			Expect(record.Data["datetime"]).NotTo(BeNil())
			Expect(record.Data["date"]).NotTo(BeNil())
			Expect(record.Data["time"]).NotTo(BeNil())
		})
	})
})
