package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"custodian/server/pg"
	"custodian/server/data"
	"custodian/server/auth"
	"custodian/utils"

	"custodian/server/transactions/file_transaction"
	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/object/meta"
	"custodian/server/transactions"
	"custodian/server/object/description"
)

var _ = Describe("PG MetaStore test", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	BeforeEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can modify object adding number field with static default integer value", func() {
		Context("having an object with number field", func() {
			metaDescription := description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{})
			Expect(err).To(BeNil())

			By("adding a number field to the object")
			metaDescription = description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "ordering",
						Type:     description.FieldTypeNumber,
						Def:      10,
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true, )
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", nil, nil, 1, false)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data["ordering"]).To(Equal(float64(10)))
			})
		})
	})

	It("can modify object adding number field with static default float value", func() {
		Context("having an object with number field", func() {
			metaDescription := description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{})
			Expect(err).To(BeNil())

			By("adding a number field to the object")
			metaDescription = description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "ordering",
						Type:     description.FieldTypeNumber,
						Def:      10.98,
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", nil, nil, 1, false)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data["ordering"]).To(Equal(float64(10.98)))
			})
		})
	})

	It("can modify object adding boolean field with static default boolean value", func() {
		Context("having an object with bool field", func() {
			metaDescription := description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{})
			Expect(err).To(BeNil())

			By("adding a boolean field to the object")
			metaDescription = description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "is_active",
						Type:     description.FieldTypeBool,
						Def:      true,
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", nil, nil, 1, false)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data["is_active"]).To(BeTrue())
			})
		})
	})

	It("can modify object adding string field with static default string value", func() {
		Context("having an object with string field", func() {
			metaDescription := description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{})
			Expect(err).To(BeNil())

			By("adding a string field to the object")
			metaDescription = description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "name",
						Type:     description.FieldTypeString,
						Def:      "Not specified",
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", nil, nil, 1, false)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data["name"]).To(Equal("Not specified"))
			})
		})
	})

	It("can modify object by adding date field with static default string value", func() {
		Context("having an object with string field", func() {
			metaDescription := description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{})
			Expect(err).To(BeNil())

			By("adding a date field to the object")
			metaDescription = description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "date",
						Type:     description.FieldTypeDate,
						Def:      "2018-05-22",
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", nil, nil, 1, false)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data["date"]).To(Equal("2018-05-22"))
			})
		})
	})

	It("can modify object by adding datetime field with static default string value", func() {
		Context("having an object with string field", func() {
			metaDescription := description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{})
			Expect(err).To(BeNil())

			By("adding a datetime field to the object")
			metaDescription = description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "datetime",
						Type:     description.FieldTypeDateTime,
						Def:      "2018-05-29T15:29:58.627755+05:00",
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", nil, nil, 1, false)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data["datetime"]).To(Equal("2018-05-29T10:29:58.627755Z"))
			})
		})
	})

	It("can modify object by adding time field with static default string value", func() {
		Context("having an object with string field", func() {
			metaDescription := description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaCreateError := metaStore.Create(metaObj)
			Expect(metaCreateError).To(BeNil())

			By("creating record of this object")
			_, err = dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"id": 44}, auth.User{})
			Expect(err).To(BeNil())

			By("adding a time field to the object")
			metaDescription = description.MetaDescription{
				Name: "someobject",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: false,
					},
					{
						Name:     "time",
						Type:     description.FieldTypeTime,
						Def:      "15:29:58+07:00",
						Optional: false,
					},
				},
			}
			metaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true)
			Expect(err).To(BeNil())

			Context("existing record`s value should equal to default value", func() {
				_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "eq(id,44)", nil, nil, 1, false)
				Expect(matchedRecords).To(HaveLen(1))

				Expect(matchedRecords[0].Data["time"]).To(Equal("15:29:58+07:00"))
			})
		})
	})
})
