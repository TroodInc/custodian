package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"server/pg"
	"server/data"
	"server/auth"
	"utils"

	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/object/meta"
	"server/transactions"
	"server/object/description"
)

var _ = Describe("PG Auto Values Test", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	BeforeEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	Context("Having an object with fields with autoOnUpdate set to true", func() {
		var metaObj *meta.Meta

		BeforeEach(func() {
			var err error
			metaDescription := description.GetBasicMetaDescription("random")
			metaDescription.Fields = append(metaDescription.Fields, []description.Field{
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
			}...)
			metaObj, err = metaStore.NewMeta(metaDescription)
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
		var metaObj *meta.Meta

		BeforeEach(func() {
			var err error
			metaDescription := description.GetBasicMetaDescription("random")
			metaDescription.Fields = append(metaDescription.Fields, []description.Field{
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
			}...)
			metaObj, err = metaStore.NewMeta(metaDescription)
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
