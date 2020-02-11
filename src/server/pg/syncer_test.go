package pg_test

import (
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object"
	"server/pg"
	"utils"

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
	metaStore := object.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("Changes field`s 'optional' attribute from 'false' to 'true' with corresponding database column altering", func() {
		Describe("Having an object with required field", func() {
			//create meta
			metaDescription := object.Meta{
				Name: "person",
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
					}, {
						Name:     "name",
						Type:     object.FieldTypeString,
						Optional: false,
					},
				},
			}
			objectMeta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())

			err = metaStore.Create(objectMeta)
			Expect(err).To(BeNil())

			Describe("this field is specified as optional and object is updated", func() {
				metaDescription = object.Meta{
					Name: "person",
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
						}, {
							Name:     "name",
							Type:     object.FieldTypeString,
							Optional: true,
						},
					},
				}
				objectMeta, err := metaStore.NewMeta(&metaDescription)
				Expect(err).To(BeNil())

				_, err = metaStore.Update(objectMeta.Name, objectMeta, true)
				Expect(err).To(BeNil())

				globalTransaction, err := globalTransactionManager.BeginTransaction()
				tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
				metaDdl, err := pg.MetaDDLFromDB(tx, objectMeta.Name)
				globalTransactionManager.CommitTransaction(globalTransaction)

				Expect(err).To(BeNil())
				Expect(metaDdl.Columns[1].Optional).To(BeTrue())
			})
		})

	})

	It("Changes field`s 'optional' attribute from 'true' to 'false' with corresponding database column altering", func() {
		Describe("Having an object with required field", func() {
			//create meta
			metaDescription := object.Meta{
				Name: "person",
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
					}, {
						Name:     "name",
						Type:     object.FieldTypeString,
						Optional: true,
					},
				},
			}
			objectMeta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectMeta)
			Expect(err).To(BeNil())

			Describe("this field is specified as optional and object is updated", func() {
				metaDescription = object.Meta{
					Name: "person",
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
						}, {
							Name:     "name",
							Type:     object.FieldTypeString,
							Optional: false,
						},
					},
				}
				objectMeta, err := metaStore.NewMeta(&metaDescription)
				Expect(err).To(BeNil())
				_, err = metaStore.Update(objectMeta.Name, objectMeta, true)
				Expect(err).To(BeNil())

				globalTransaction, err := globalTransactionManager.BeginTransaction()
				tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
				metaDdl, err := pg.MetaDDLFromDB(tx, objectMeta.Name)
				globalTransactionManager.CommitTransaction(globalTransaction)

				Expect(err).To(BeNil())
				Expect(metaDdl.Columns[1].Optional).To(BeFalse())
			})
		})

	})
})
