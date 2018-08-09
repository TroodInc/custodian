package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
	"server/pg"
	"server/data"
	"server/auth"
	"utils"
)

var _ = Describe("Data", func() {
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

	It("can create a record containing null value of foreign key field", func() {
		Context("having Reason object", func() {
			reasonMetaDescription := object.MetaDescription{
				Name: "reason",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeString,
						Optional: true,
					},
				},
			}
			reasonMetaObj, err := metaStore.NewMeta(&reasonMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(reasonMetaObj)
			Expect(err).To(BeNil())

			Context("and Lead object referencing Reason object", func() {
				leadMetaDescription := object.MetaDescription{
					Name: "lead",
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
							Name: "name",
							Type: object.FieldTypeString,
						},
						{
							Name:     "decline_reason",
							Type:     object.FieldTypeObject,
							Optional: true,
							LinkType: object.LinkTypeInner,
							LinkMeta: "reason",
						},
					},
				}
				leadMetaObj, err := metaStore.NewMeta(&leadMetaDescription)
				Expect(err).To(BeNil())
				metaStore.Create(leadMetaObj)
				Context("Lead record with empty reason is created", func() {
					leadData := map[string]interface{}{
						"name": "newLead",
					}
					user := auth.User{}
					record, err := dataProcessor.CreateRecord(leadMetaDescription.Name, leadData, user, true)
					Expect(err).To(BeNil())
					Expect(record).To(HaveKey("decline_reason"))
				})
			})
		})
	})

	It("can create record without specifying any value", func() {
		Context("having an object with optional fields", func() {
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
						Name:     "name",
						Type:     object.FieldTypeString,
						Optional: true,
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			Context("DataManager creates record without any values", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{}, auth.User{}, true)
				Expect(err).To(BeNil())
				Expect(record["id"]).To(BeEquivalentTo(1))
			})
		})
	})

	It("can create record with null value for optional field", func() {
		Context("having an object", func() {
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
						Name:     "name",
						Type:     object.FieldTypeString,
						Optional: true,
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			recordOne, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": nil}, auth.User{}, true)
			Expect(err).To(BeNil())
			Expect(recordOne["name"]).To(BeNil())
		})
	})

	It("Can create records containing reserved words", func() {
		Context("having an object named by reserved word and containing field named by reserved word", func() {
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
						Name: "order",
						Type: object.FieldTypeString,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())
			Context("and record has values containing reserved word", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"order": "order"}, auth.User{}, true)
				Expect(err).To(BeNil())
				Expect(record["id"]).To(Equal(float64(1)))

			})

		})

	})

	It("Can insert numeric value into string field", func() {
		Context("having an object with string field", func() {
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
						Name: "name",
						Type: object.FieldTypeString,
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			Context("record can contain numeric value for string field", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": 202}, auth.User{}, true)
				Expect(err).To(BeNil())
				Expect(record["name"]).To(Equal("202"))
			})

		})

	})
})
