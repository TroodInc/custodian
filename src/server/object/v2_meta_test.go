package object

import (
	"encoding/json"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/description"
	"server/object/meta"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/transactions/file_transaction"
	"utils"
)

var _ = Describe("Refactoring Meta", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("must JSON marshal v2meta same as old meta", func() {
		oldMeta, err := metaStore.NewMeta(&description.MetaDescription{
			Name: "person",
			Key: "id",
			Cas: false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				}, {
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: false,
				},
			},
		})

		Expect(err).To(BeNil())

		v2Meta, err := metaStore.V2NewMeta(
			"person",
			"id",
			false,
			[]*description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				}, {
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: false,
				},
			},
			nil,
		)
		Expect(err).To(BeNil())

		oldJSON, err := json.Marshal(oldMeta)
		Expect(err).To(BeNil())

		v2JSON, err := json.Marshal(v2Meta)
		Expect(err).To(BeNil())

		Expect(oldJSON).To(Equal(v2JSON))
	})

	It("V2 can create meta", func() {
		meta, err := metaStore.V2NewMeta(
			"person",
			"id",
			false,
			[]*description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				}, {
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: false,
				},
			},
			nil,
		)
		Expect(err).To(BeNil())
		err = metaStore.V2Create(meta)
		Expect(err).To(BeNil())
	})

})