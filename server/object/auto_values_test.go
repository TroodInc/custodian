package object_test

import (
	"custodian/server/object"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"custodian/server/auth"
	"custodian/utils"

	"custodian/server/object/description"
)

var _ = Describe("PG Auto Values Test", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache(), db)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)
	dataProcessor, _ := object.NewProcessor(metaStore, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})
	testObjAName := utils.RandomString(8)

	Context("Having an object with fields with autoOnUpdate set to true", func() {
		var metaObj *object.Meta

		BeforeEach(func() {
			var err error
			metaDescription := description.MetaDescription{
				Name: testObjAName,
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:        "datetime",
						Type:        description.FieldTypeDateTime,
						NowOnUpdate: true,
						Optional:    true,
					},
					{
						Name:        "date",
						Type:        description.FieldTypeDate,
						NowOnUpdate: true,
						Optional:    true,
					},
					{
						Name:        "time",
						Type:        description.FieldTypeTime,
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
			metaDescription := description.MetaDescription{
				Name: testObjAName,
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:        "datetime",
						Type:        description.FieldTypeDateTime,
						NowOnCreate: true,
						Optional:    true,
					},
					{
						Name:        "date",
						Type:        description.FieldTypeDate,
						NowOnCreate: true,
						Optional:    true,
					},
					{
						Name:        "time",
						Type:        description.FieldTypeTime,
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
