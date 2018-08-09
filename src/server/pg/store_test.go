package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
	"server/pg"
	"server/data"
	"server/auth"
	"utils"
)

var _ = Describe("Store", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := object.NewStore(object.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)
	user := auth.User{1, "staff", "active", "manager"}

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	It("Having an record for person with null value", func() {
		//create meta
		meta := object.MetaDescription{
			Name: "person",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				}, {
					Name:     "name",
					Type:     object.FieldTypeString,
					Optional: false,
				}, {
					Name:     "gender",
					Type:     object.FieldTypeString,
					Optional: true,
				},
			},
		}
		metaDescription, _ := metaStore.NewMeta(&meta)

		err := metaStore.Create(metaDescription)
		Expect(err).To(BeNil())

		//create record
		recordData := map[string]interface{}{
			"name": "Sergey",
		}
		record, _ := dataProcessor.CreateRecord(meta.Name, recordData, user, true)
		Expect(record).To(HaveKey("gender"))
	})
})
