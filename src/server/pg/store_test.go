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
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

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
		meta := meta.MetaDescription{
			Name: "person",
			Key:  "id",
			Cas:  false,
			Fields: []meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				}, {
					Name:     "name",
					Type:     meta.FieldTypeString,
					Optional: false,
				}, {
					Name:     "gender",
					Type:     meta.FieldTypeString,
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
		record, _ := dataProcessor.Put(meta.Name, recordData, user)
		Expect(record).To(HaveKey("gender"))
	})
})
