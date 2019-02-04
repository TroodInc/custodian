package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"server/pg"
	"server/data"
	"server/auth"
	"utils"
	"regexp"

	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/object/meta"
	"server/transactions"
	"server/object/description"
)

var _ = Describe("PG MetaStore test", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	var globalTransaction *transactions.GlobalTransaction

	BeforeEach(func() {
		var err error

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

	})

	AfterEach(func() {
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("can create object with fields containing reserved words", func() {
		Context("Once create method is called with an object containing fields with reserved words", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
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
						Name:     "select",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
			}
			meta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, meta)
			Expect(err).To(BeNil())
			object, _, err := metaStore.Get(globalTransaction, metaDescription.Name, true)
			Expect(err).To(BeNil())
			Expect(object.Name).To(BeEquivalentTo(metaDescription.Name))
		})
	})

	It("can remove object with fields containing reserved words", func() {
		Context("once create method is called with an object containing fields with reserved words", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
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
						Name:     "select",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
			}
			meta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, meta)
			Expect(err).To(BeNil())
			_, err = metaStore.Remove(globalTransaction, metaDescription.Name, true)
			Expect(err).To(BeNil())
			_, objectRetrieved, err := metaStore.Get(globalTransaction, metaDescription.Name, true)
			Expect(err).To(Not(BeNil()))

			Expect(objectRetrieved).To(BeEquivalentTo(false))
		})
	})

	It("can add field containing reserved words", func() {
		Context("once 'create' method is called with an object", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
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
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(globalTransaction, metaObj)
			Context("and 'update' method is called with an object containing fields with reserved words", func() {
				updatedMetaDescription := description.MetaDescription{
					Name: "order",
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
							Name:     "select",
							Type:     description.FieldTypeString,
							Optional: false,
						},
					},
				}
				updatedMetaObj, _ := metaStore.NewMeta(&updatedMetaDescription)
				_, err := metaStore.Update(globalTransaction, updatedMetaDescription.Name, updatedMetaObj, true)
				Expect(err).To(BeNil())
				metaObj, _, err := metaStore.Get(globalTransaction, metaDescription.Name, true)
				Expect(err).To(BeNil())

				Expect(len(metaObj.Fields)).To(BeEquivalentTo(2))
			})
		})
	})

	It("can remove field containing reserved words", func() {
		Context("once 'create' method is called with an object containing fields with reserved words", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
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
					},
					{
						Name:     "select",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, metaObj)
			Expect(err).To(BeNil())
			Context("and 'remove' method is called", func() {
				updatedMetaDescription := description.MetaDescription{
					Name: "order",
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
						},
					},
				}
				updatedMetaObj, err := metaStore.NewMeta(&updatedMetaDescription)
				Expect(err).To(BeNil())
				metaStore.Update(globalTransaction, updatedMetaDescription.Name, updatedMetaObj, true)
				metaObj, _, err = metaStore.Get(globalTransaction, metaDescription.Name, true)
				Expect(err).To(BeNil())

				Expect(len(metaObj.Fields)).To(BeEquivalentTo(1))
			})
		})
	})

	It("can create object containing date field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'date' type", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "date",
						Type:     description.FieldTypeDate,
						Optional: true,
						Def: map[string]interface{}{
							"func": "CURRENT_DATE",
						},
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(globalTransaction, metaObj)
			Expect(metaCreateError).To(BeNil())
			Context("and record is created", func() {
				record, recordCreateError := dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaObj.Name, map[string]interface{}{}, auth.User{})
				Expect(recordCreateError).To(BeNil())
				matched, _ := regexp.MatchString("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", record.Data["date"].(string))
				Expect(matched).To(BeTrue())
			})
		})
	})

	It("can create object containing time field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'time' type", func() {
			metaDescription := description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "time",
						Type:     description.FieldTypeTime,
						Optional: true,
						Def: map[string]interface{}{
							"func": "now",
						},
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(globalTransaction, metaObj)
			Expect(metaCreateError).To(BeNil())
			Context("and record is created", func() {
				record, recordCreateError := dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaObj.Name, map[string]interface{}{}, auth.User{})
				Expect(recordCreateError).To(BeNil())
				matched, _ := regexp.MatchString("^[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]+Z$", record.Data["time"].(string))
				Expect(matched).To(BeTrue())
			})
		})
	})

	It("can create object containing datetime field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'datetime' type", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "created",
						Type:     description.FieldTypeDateTime,
						Optional: true,
						Def: map[string]interface{}{
							"func": "CURRENT_TIMESTAMP",
						},
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(globalTransaction, metaObj)
			Expect(metaCreateError).To(BeNil())
			Context("and record is created", func() {
				record, recordCreateError := dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaObj.Name, map[string]interface{}{}, auth.User{})
				Expect(recordCreateError).To(BeNil())
				matched, _ := regexp.MatchString("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]+Z$", record.Data["created"].(string))
				Expect(matched).To(BeTrue())
			})
		})
	})

	It("can create object containing datetime field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'datetime' type", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(globalTransaction, metaObj)
			Expect(metaCreateError).To(BeNil())
			Context("and record is created", func() {
				_, recordCreateError := dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaObj.Name, map[string]interface{}{}, auth.User{})
				Expect(recordCreateError).To(BeNil())
				Context("Mandatory field added", func() {
					updatedMetaDescription := description.MetaDescription{
						Name: "order",
						Key:  "id",
						Cas:  false,
						Fields: []description.Field{
							{
								Name:     "id",
								Type:     description.FieldTypeNumber,
								Optional: true,
								Def: map[string]interface{}{
									"func": "nextval",
								},
							},
							{
								Name:     "created",
								Type:     description.FieldTypeDateTime,
								Optional: false,
								Def: map[string]interface{}{
									"func": "CURRENT_TIMESTAMP",
								},
							},
						},
					}
					metaObj, err := metaStore.NewMeta(&updatedMetaDescription)
					Expect(err).To(BeNil())
					ok, err := metaStore.Update(globalTransaction, metaObj.Name, metaObj, true)
					Expect(ok).To(BeTrue())
					Expect(err).To(BeNil())
				})
			})
		})
	})

	It("can query object containing reserved words", func() {
		Context("once 'create' method is called with an object containing fields with reserved words", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
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
					},
					{
						Name:     "order",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, metaObj)
			Expect(err).To(BeNil())

			_, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaObj.Name, map[string]interface{}{"order": "value"}, auth.User{})
			Expect(err).To(BeNil())

			record, err := dataProcessor.Get(globalTransaction.DbTransaction, metaObj.Name, "1", nil, nil, 1, false)
			Expect(err).To(BeNil())
			Expect(record.Data["order"]).To(Equal("value"))

		})
	})
})
