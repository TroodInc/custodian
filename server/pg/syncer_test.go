package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"custodian/server/pg"
	"database/sql"
	"custodian/utils"
	"custodian/server/transactions/file_transaction"

	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions"
	"custodian/server/object/meta"
	"custodian/server/object/description"
)

var _ = Describe("Store", func() {
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

	It("Changes field`s 'optional' attribute from 'false' to 'true' with corresponding database column altering", func() {
		Describe("Having an object with required field", func() {
			//create meta
			metaDescription := description.MetaDescription{
				Name: "person",
				Key:  "id",
				Cas:  false,
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
			}
			objectMeta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())

			err = metaStore.Create(objectMeta)
			Expect(err).To(BeNil())

			Describe("this field is specified as optional and object is updated", func() {
				metaDescription = description.MetaDescription{
					Name: "person",
					Key:  "id",
					Cas:  false,
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
							Optional: true,
						},
					},
				}
				objectMeta, err := metaStore.NewMeta(&metaDescription)
				Expect(err).To(BeNil())

				_, err = metaStore.Update(objectMeta.Name, objectMeta, true)
				Expect(err).To(BeNil())

				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
				metaDdl, err := pg.MetaDDLFromDB(tx, objectMeta.Name)
				globalTransactionManager.CommitTransaction(globalTransaction)

				Expect(err).To(BeNil())
				Expect(metaDdl.Columns[1].Optional).To(BeTrue())
			})

			Describe("can update this field to camelCase", func() {
				metaDescription = description.MetaDescription{
					Name: "person",
					Key:  "id",
					Cas:  false,
					Fields: []description.Field{
						{
							Name: "id",
							Type: description.FieldTypeNumber,
							Def: map[string]interface{}{
								"func": "nextval",
							},
							Optional: true,
						}, {
							Name:     "Name",
							Type:     description.FieldTypeString,
							Optional: true,
						},
					},
				}
				objectMeta, err := metaStore.NewMeta(&metaDescription)
				Expect(err).To(BeNil())

				_, err = metaStore.Update(objectMeta.Name, objectMeta, true)
				Expect(err).To(BeNil())

				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
				metaDdl, err := pg.MetaDDLFromDB(tx, objectMeta.Name)
				globalTransactionManager.CommitTransaction(globalTransaction)

				Expect(err).To(BeNil())
				Expect(metaDdl.Columns[1].Name).To(Equal("Name"))
			})
		})

	})

	It("Changes field`s 'optional' attribute from 'true' to 'false' with corresponding database column altering", func() {
		Describe("Having an object with required field", func() {
			//create meta
			metaDescription := description.MetaDescription{
				Name: "person",
				Key:  "id",
				Cas:  false,
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
						Optional: true,
					},
				},
			}
			objectMeta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectMeta)
			Expect(err).To(BeNil())

			Describe("this field is specified as optional and object is updated", func() {
				metaDescription = description.MetaDescription{
					Name: "person",
					Key:  "id",
					Cas:  false,
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
				}
				objectMeta, err := metaStore.NewMeta(&metaDescription)
				Expect(err).To(BeNil())
				_, err = metaStore.Update(objectMeta.Name, objectMeta, true)
				Expect(err).To(BeNil())

				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
				metaDdl, err := pg.MetaDDLFromDB(tx, objectMeta.Name)
				globalTransactionManager.CommitTransaction(globalTransaction)

				Expect(err).To(BeNil())
				Expect(metaDdl.Columns[1].Optional).To(BeFalse())
			})
		})

	})
})
