package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
	"server/pg"
	"server/data"
	"server/auth"
)

var _ = Describe("Store", func() {
	databaseConnectionOptions := "host=localhost dbname=custodian sslmode=disable"
	syncer, _ := pg.NewSyncer(databaseConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)
	user := auth.User{1, "staff", "active", "manager"}
	metaStore.Flush()

	Describe("Having an record for person with null value", func() {
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
		metaStore.Create(metaDescription)

		//create record
		recordData := map[string]interface{}{
			"name": "Sergey",
		}
		record, _ := dataProcessor.Put(meta.Name, recordData, user)
		It("DataProcess returns a list containing one record", func() {
			Expect(record).To(HaveKey("gender"))
		})

	})
})
