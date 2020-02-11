package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"regexp"
	"server/auth"
	"server/data"
	meta2 "server/object/meta"
	"server/pg"
	"utils"

	pg_transactions "server/pg/transactions"
	"server/transactions"
)

var _ = Describe("PG MetaStore test", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta2.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can create object with fields containing reserved words", func() {
		Context("Once create method is called with an object containing fields with reserved words", func() {
			metaDescription := meta2.Meta{
				Name: "order",
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
						Name:     "select",
						Type:     meta2.FieldTypeString,
						Optional: false,
					},
				},
			}
			meta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(meta)
			Expect(err).To(BeNil())
			object, _, err := metaStore.Get(metaDescription.Name, true)
			Expect(err).To(BeNil())
			Expect(object.Name).To(BeEquivalentTo(metaDescription.Name))
		})
	})

	It("can remove object with fields containing reserved words", func() {
		Context("once create method is called with an object containing fields with reserved words", func() {
			metaDescription := meta2.Meta{
				Name: "order",
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
						Name:     "select",
						Type:     meta2.FieldTypeString,
						Optional: false,
					},
				},
			}
			meta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(meta)
			Expect(err).To(BeNil())
			_, err = metaStore.Remove(metaDescription.Name, true)
			Expect(err).To(BeNil())
			_, objectRetrieved, err := metaStore.Get(metaDescription.Name, true)
			Expect(err).To(Not(BeNil()))

			Expect(objectRetrieved).To(BeEquivalentTo(false))
		})
	})

	It("can add field containing reserved words", func() {
		Context("once 'create' method is called with an object", func() {
			metaDescription := meta2.Meta{
				Name: "order",
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
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)
			Context("and 'update' method is called with an object containing fields with reserved words", func() {
				updatedMetaDescription := meta2.Meta{
					Name: "order",
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
							Name:     "select",
							Type:     meta2.FieldTypeString,
							Optional: false,
						},
					},
				}
				updatedMetaObj, _ := metaStore.NewMeta(&updatedMetaDescription)
				_, err := metaStore.Update(updatedMetaDescription.Name, updatedMetaObj, true)
				Expect(err).To(BeNil())
				metaObj, _, err := metaStore.Get(metaDescription.Name, true)
				Expect(err).To(BeNil())

				Expect(len(metaObj.Fields)).To(BeEquivalentTo(2))
			})
		})
	})

	It("can remove field containing reserved words", func() {
		Context("once 'create' method is called with an object containing fields with reserved words", func() {
			metaDescription := meta2.Meta{
				Name: "order",
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
					},
					{
						Name:     "select",
						Type:     meta2.FieldTypeString,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())
			Context("and 'remove' method is called", func() {
				updatedMetaDescription := meta2.Meta{
					Name: "order",
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
						},
					},
				}
				updatedMetaObj, err := metaStore.NewMeta(&updatedMetaDescription)
				Expect(err).To(BeNil())
				metaStore.Update(updatedMetaDescription.Name, updatedMetaObj, true)
				metaObj, _, err = metaStore.Get(metaDescription.Name, true)
				Expect(err).To(BeNil())

				Expect(len(metaObj.Fields)).To(BeEquivalentTo(1))
			})
		})
	})

	It("can create object containing date field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'date' type", func() {
			metaDescription := meta2.Meta{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []*meta2.Field{
					{
						Name:     "id",
						Type:     meta2.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:        "date",
						Type:        meta2.FieldTypeDate,
						Optional:    true,
						NowOnCreate: true,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())
			Context("and record is created", func() {
				record, recordCreateError := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{}, auth.User{})
				Expect(recordCreateError).To(BeNil())
				matched, _ := regexp.MatchString("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", record.Data["date"].(string))
				Expect(matched).To(BeTrue())
			})
		})
	})

	It("can create object containing time field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'time' type", func() {
			metaDescription := meta2.Meta{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []*meta2.Field{
					{
						Name:     "id",
						Type:     meta2.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "time",
						Type:     meta2.FieldTypeTime,
						Optional: true,
						Def: map[string]interface{}{
							"func": "now",
						},
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())
			Context("and record is created", func() {
				record, recordCreateError := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{}, auth.User{})
				Expect(recordCreateError).To(BeNil())
				matched, _ := regexp.MatchString("^[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]+Z$", record.Data["time"].(string))
				Expect(matched).To(BeTrue())
			})
		})
	})

	It("can create object containing datetime field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'datetime' type", func() {
			metaDescription := meta2.Meta{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []*meta2.Field{
					{
						Name:     "id",
						Type:     meta2.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:        "created",
						Type:        meta2.FieldTypeDateTime,
						Optional:    true,
						NowOnCreate: true,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())
			Context("and record is created", func() {
				record, recordCreateError := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{}, auth.User{})
				Expect(recordCreateError).To(BeNil())
				matched, _ := regexp.MatchString("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]+Z$", record.Data["created"].(string))
				Expect(matched).To(BeTrue())
			})
		})
	})

	It("can create object containing datetime field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'datetime' type", func() {
			metaDescription := meta2.Meta{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []*meta2.Field{
					{
						Name:     "id",
						Type:     meta2.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())
			Context("and record is created", func() {
				_, recordCreateError := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{}, auth.User{})
				Expect(recordCreateError).To(BeNil())
				Context("Mandatory field added", func() {
					updatedMetaDescription := meta2.Meta{
						Name: "order",
						Key:  "id",
						Cas:  false,
						Fields: []*meta2.Field{
							{
								Name:     "id",
								Type:     meta2.FieldTypeNumber,
								Optional: true,
								Def: map[string]interface{}{
									"func": "nextval",
								},
							},
							{
								Name:        "created",
								Type:        meta2.FieldTypeDateTime,
								Optional:    true,
								NowOnCreate: true,
							},
						},
					}
					metaObj, err := metaStore.NewMeta(&updatedMetaDescription)
					Expect(err).To(BeNil())
					ok, err := metaStore.Update(metaObj.Name, metaObj, true)
					Expect(ok).To(BeTrue())
					Expect(err).To(BeNil())
				})
			})
		})
	})

	It("can query object containing reserved words", func() {
		Context("once 'create' method is called with an object containing fields with reserved words", func() {
			metaDescription := meta2.Meta{
				Name: "order",
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
					},
					{
						Name:     "order",
						Type:     meta2.FieldTypeString,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"order": "value"}, auth.User{})
			Expect(err).To(BeNil())

			record, err := dataProcessor.Get(metaObj.Name, "1", nil, nil, 1, false)
			Expect(err).To(BeNil())
			Expect(record.Data["order"]).To(Equal("value"))

		})
	})
})
