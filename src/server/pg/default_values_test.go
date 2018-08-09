package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"server/pg"
	"server/meta"
	"server/data"
	"server/auth"
	"utils"
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

	It("can modify object adding number field with static default integer value", func() {
		Context("having an object with number field", func() {
			metaDescription := object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{}, true)
			Expect(err).To(BeNil())

			By("adding a number field to the object")
			metaDescription = object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "ordering",
						Type:     object.FieldTypeNumber,
						Def:      10,
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true, true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				matchedRecords := []map[string]interface{}{}
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", 1, callbackFunction, true)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0]["ordering"]).To(Equal(float64(10)))
			})
		})
	})

	It("can modify object adding number field with static default float value", func() {
		Context("having an object with number field", func() {
			metaDescription := object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{}, true)
			Expect(err).To(BeNil())

			By("adding a number field to the object")
			metaDescription = object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "ordering",
						Type:     object.FieldTypeNumber,
						Def:      10.98,
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true,true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				matchedRecords := []map[string]interface{}{}
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", 1, callbackFunction, true)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0]["ordering"]).To(Equal(float64(10.98)))
			})
		})
	})

	It("can modify object adding boolean field with static default boolean value", func() {
		Context("having an object with bool field", func() {
			metaDescription := object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{}, true)
			Expect(err).To(BeNil())

			By("adding a boolean field to the object")
			metaDescription = object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "is_active",
						Type:     object.FieldTypeBool,
						Def:      true,
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true,true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				matchedRecords := []map[string]interface{}{}
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", 1, callbackFunction, true)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0]["is_active"]).To(BeTrue())
			})
		})
	})

	It("can modify object adding string field with static default string value", func() {
		Context("having an object with string field", func() {
			metaDescription := object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{}, true)
			Expect(err).To(BeNil())

			By("adding a string field to the object")
			metaDescription = object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "name",
						Type:     object.FieldTypeString,
						Def:      "Not specified",
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true,true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				matchedRecords := []map[string]interface{}{}
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", 1, callbackFunction, true)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0]["name"]).To(Equal("Not specified"))
			})
		})
	})

	It("can modify object by adding date field with static default string value", func() {
		Context("having an object with string field", func() {
			metaDescription := object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{}, true)
			Expect(err).To(BeNil())

			By("adding a date field to the object")
			metaDescription = object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "date",
						Type:     object.FieldTypeDate,
						Def:      "2018-05-22",
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true,true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				matchedRecords := []map[string]interface{}{}
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", 1, callbackFunction, true)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0]["date"]).To(Equal("2018-05-22"))
			})
		})
	})

	It("can modify object by adding datetime field with static default string value", func() {
		Context("having an object with string field", func() {
			metaDescription := object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{}, true)
			Expect(err).To(BeNil())

			By("adding a datetime field to the object")
			metaDescription = object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "datetime",
						Type:     object.FieldTypeDateTime,
						Def:      "2018-05-29T15:29:58.627755+05:00",
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true,true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				matchedRecords := []map[string]interface{}{}
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", 1, callbackFunction, true)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0]["datetime"]).To(Equal("2018-05-29T15:29:58.627755+05:00"))
			})
		})
	})

	It("can modify object by adding time field with static default string value", func() {
		Context("having an object with string field", func() {
			metaDescription := object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{}, true)
			Expect(err).To(BeNil())

			By("adding a time field to the object")
			metaDescription = object.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "time",
						Type:     object.FieldTypeTime,
						Def:      "15:29:58+07:00",
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true,true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				matchedRecords := []map[string]interface{}{}
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", 1, callbackFunction, true)
				Expect(matchedRecords).To(HaveLen(1))

				Expect(matchedRecords[0]["time"]).To(Equal("15:29:58+07:00"))
			})
		})
	})
})
