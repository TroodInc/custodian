package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"server/pg"
	"server/meta"
	"server/data"
	"server/auth"
	"utils"
	"regexp"
)

var _ = Describe("PG MetaStore test", func() {
	appConfig := utils.GetConfig()

	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := object.NewStore(object.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)

	BeforeEach(func() {
		metaStore.Flush()
	})
	AfterEach(func() {
		metaStore.Flush()
	})

	It("can create object with fields containing reserved words", func() {
		Context("Once create method is called with an object containing fields with reserved words", func() {
			metaDescription := object.MetaDescription{
				Name: "order",
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
						Name:     "select",
						Type:     object.FieldTypeString,
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
			metaDescription := object.MetaDescription{
				Name: "order",
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
						Name:     "select",
						Type:     object.FieldTypeString,
						Optional: false,
					},
				},
			}
			meta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(meta)
			Expect(err).To(BeNil())
			_, err = metaStore.Remove(metaDescription.Name, true, true)
			Expect(err).To(BeNil())
			_, objectRetrieved, err := metaStore.Get(metaDescription.Name, true)
			Expect(err).To(Not(BeNil()))

			Expect(objectRetrieved).To(BeEquivalentTo(false))
		})
	})

	It("can add field containing reserved words", func() {
		Context("once 'create' method is called with an object", func() {
			metaDescription := object.MetaDescription{
				Name: "order",
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
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)
			Context("and 'update' method is called with an object containing fields with reserved words", func() {
				updatedMetaDescription := object.MetaDescription{
					Name: "order",
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
							Name:     "select",
							Type:     object.FieldTypeString,
							Optional: false,
						},
					},
				}
				updatedMetaObj, _ := metaStore.NewMeta(&updatedMetaDescription)
				_, err := metaStore.Update(updatedMetaDescription.Name, updatedMetaObj, true,true)
				Expect(err).To(BeNil())
				metaObj, _, err := metaStore.Get(metaDescription.Name, true)
				Expect(err).To(BeNil())

				Expect(len(metaObj.Fields)).To(BeEquivalentTo(2))
			})
		})
	})

	It("can remove field containing reserved words", func() {
		Context("once 'create' method is called with an object containing fields with reserved words", func() {
			metaDescription := object.MetaDescription{
				Name: "order",
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
					},
					{
						Name:     "select",
						Type:     object.FieldTypeString,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())
			Context("and 'remove' method is called", func() {
				updatedMetaDescription := object.MetaDescription{
					Name: "order",
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
						},
					},
				}
				updatedMetaObj, err := metaStore.NewMeta(&updatedMetaDescription)
				Expect(err).To(BeNil())
				metaStore.Update(updatedMetaDescription.Name, updatedMetaObj, true,true)
				metaObj, _, err = metaStore.Get(metaDescription.Name, true)
				Expect(err).To(BeNil())

				Expect(len(metaObj.Fields)).To(BeEquivalentTo(1))
			})
		})
	})

	It("can create object containing date field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'date' type", func() {
			metaDescription := object.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "date",
						Type:     object.FieldTypeDate,
						Optional: true,
						Def: map[string]interface{}{
							"func": "CURRENT_DATE",
						},
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())
			Context("and record is created", func() {
				record, recordCreateError := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{}, auth.User{}, true)
				Expect(recordCreateError).To(BeNil())
				matched, _ := regexp.MatchString("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", record["date"].(string))
				Expect(matched).To(BeTrue())
			})
		})
	})

	It("can create object containing time field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'time' type", func() {
			metaDescription := object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "time",
						Type:     object.FieldTypeTime,
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
				record, recordCreateError := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{}, auth.User{}, true)
				Expect(recordCreateError).To(BeNil())
				matched, _ := regexp.MatchString("^[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]+\\+[0-9]{2}:[0-9]{2}$", record["time"].(string))
				Expect(matched).To(BeTrue())
			})
		})
	})

	It("can create object containing datetime field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'datetime' type", func() {
			metaDescription := object.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "created",
						Type:     object.FieldTypeDateTime,
						Optional: true,
						Def: map[string]interface{}{
							"func": "CURRENT_TIMESTAMP",
						},
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())
			Context("and record is created", func() {
				record, recordCreateError := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{}, auth.User{}, true)
				Expect(recordCreateError).To(BeNil())
				matched, _ := regexp.MatchString("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]+\\+[0-9]{2}:[0-9]{2}$", record["created"].(string))
				Expect(matched).To(BeTrue())
			})
		})
	})

	It("can create object containing datetime field with default value", func() {
		Context("once 'create' method is called with an object containing field with 'datetime' type", func() {
			metaDescription := object.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
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
				_, recordCreateError := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{}, auth.User{}, true)
				Expect(recordCreateError).To(BeNil())
				Context("Mandatory field added", func() {
					updatedMetaDescription := object.MetaDescription{
						Name: "order",
						Key:  "id",
						Cas:  false,
						Fields: []object.Field{
							{
								Name:     "id",
								Type:     object.FieldTypeNumber,
								Optional: true,
								Def: map[string]interface{}{
									"func": "nextval",
								},
							},
							{
								Name:     "created",
								Type:     object.FieldTypeDateTime,
								Optional: false,
								Def: map[string]interface{}{
									"func": "CURRENT_TIMESTAMP",
								},
							},
						},
					}
					metaObj, err := metaStore.NewMeta(&updatedMetaDescription)
					Expect(err).To(BeNil())
					ok, err := metaStore.Update(metaObj.Name, metaObj, true,true)
					Expect(ok).To(BeTrue())
					Expect(err).To(BeNil())
				})
			})
		})
	})

	It("can query object containing reserved words", func() {
		Context("once 'create' method is called with an object containing fields with reserved words", func() {
			metaDescription := object.MetaDescription{
				Name: "order",
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
					},
					{
						Name:     "order",
						Type:     object.FieldTypeString,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"order": "value"}, auth.User{}, true)
			Expect(err).To(BeNil())

			record, err := dataProcessor.Get(metaObj.Name, "1", 1, true)
			Expect(err).To(BeNil())
			Expect(record["order"]).To(Equal("value"))

		})
	})

})
