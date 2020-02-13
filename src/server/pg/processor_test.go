package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	meta2 "server/object/meta"
	"server/pg"
	"utils"

	"server/auth"
	"server/data"

	pg_transactions "server/pg/transactions"
	"server/transactions"
)

var _ = Describe("Store", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta2.NewMetaStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("Having an record for person with null value", func() {
		//create meta
		meta := meta2.Meta{
			Name: "person",
			Key:  "id",
			Cas:  false,
			Fields: []*meta2.Field{
				{
					Name: "id",
					Type: meta2.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				}, {
					Name:     "name",
					Type:     meta2.FieldTypeString,
					Optional: false,
				}, {
					Name:     "gender",
					Type:     meta2.FieldTypeString,
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
		record, _ := dataProcessor.CreateRecord(meta.Name, recordData, auth.User{})
		Expect(record.Data).To(HaveKey("gender"))
	})
})
