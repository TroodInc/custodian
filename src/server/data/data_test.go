package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
	"server/pg"
	"server/data"
	"server/auth"
	"fmt"
	"utils"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

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
			reasonMetaDescription := meta.MetaDescription{
				Name: "reason",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeString,
						Optional: true,
					},
				},
			}
			reasonMetaObj, err := metaStore.NewMeta(&reasonMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(reasonMetaObj)
			Expect(err).To(BeNil())

			Context("and Lead object referencing Reason object", func() {
				leadMetaDescription := meta.MetaDescription{
					Name: "lead",
					Key:  "id",
					Cas:  false,
					Fields: []meta.Field{
						{
							Name: "id",
							Type: meta.FieldTypeNumber,
							Def: map[string]interface{}{
								"func": "nextval",
							},
							Optional: true,
						},
						{
							Name: "name",
							Type: meta.FieldTypeString,
						},
						{
							Name:     "decline_reason",
							Type:     meta.FieldTypeObject,
							Optional: true,
							LinkType: meta.LinkTypeInner,
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

	It("can query records by date field", func() {
		Context("having an object with date field", func() {
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name: "date",
						Type: meta.FieldTypeDate,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			Context("and two records with dates that differ by a week", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"date": "2018-05-29"}, auth.User{}, true)
				dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"date": "2018-05-22"}, auth.User{}, true)
				matchedRecords := []map[string]interface{}{}
				Expect(err).To(BeNil())
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				Context("query by date returns correct result", func() {
					dataProcessor.GetBulk(metaObj.Name, "gt(date,2018-05-23)", 1, callbackFunction, true)
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0]["id"]).To(Equal(record["id"]))
				})
			})

		})
	})

	It("can query records by string PK value", func() {
		Context("having an A object with string PK field", func() {
			metaDescription := meta.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeString,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			By("having two records of this object")

			_, err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"id": "PKVALUE"}, auth.User{}, true)
			Expect(err).To(BeNil())

			_, err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"id": "ANOTHERPKVALUE"}, auth.User{}, true)
			Expect(err).To(BeNil())

			matchedRecords := []map[string]interface{}{}
			callbackFunction := func(obj map[string]interface{}) error {
				matchedRecords = append(matchedRecords, obj)
				return nil
			}

			By("having another object, containing A object as a link")

			metaDescription = meta.MetaDescription{
				Name: "b",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeString,
						Optional: false,
					},
					{
						Name:     "a",
						Type:     meta.FieldTypeObject,
						LinkType: meta.LinkTypeInner,
						LinkMeta: "a",
						Optional: true,
					},
				},
			}
			bMetaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())

			By("having a record of B object")
			_, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"id": "id", "a": "PKVALUE"}, auth.User{}, true)
			Expect(err).To(BeNil())

			Context("query by PK returns correct result", func() {
				dataProcessor.GetBulk(bMetaObj.Name, "eq(a,PKVALUE)", 1, callbackFunction, true)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0]["id"]).To(Equal("id"))
			})

		})
	})

	It("can query records by datetime field", func() {
		Context("having an object with datetime field", func() {
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name: "created",
						Type: meta.FieldTypeDateTime,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			Context("and two records with 'created' values that differ by a week", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"created": "2018-05-29T15:29:58.627755+05:00"}, auth.User{}, true)
				Expect(err).To(BeNil())
				dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"created": "2018-05-22T15:29:58.627755+05:00"}, auth.User{}, true)
				matchedRecords := []map[string]interface{}{}
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				Context("query by 'created' field returns correct result", func() {
					dataProcessor.GetBulk(metaObj.Name, "gt(created,2018-05-23)", 1, callbackFunction, true)
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0]["id"]).To(Equal(record["id"]))
				})
			})

		})
	})

	It("can query records by time field", func() {
		Context("having an object with datetime field", func() {
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name: "created_time",
						Type: meta.FieldTypeTime,
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			Context("and two records with 'created_time' values that differ by several hours", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"created_time": "14:00:00 +05:00"}, auth.User{}, true)
				dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"created_time": "09:00:00 +05:00"}, auth.User{}, true)
				matchedRecords := []map[string]interface{}{}
				Expect(err).To(BeNil())
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				Context("query by 'created' field returns correct result", func() {
					//query by value greater than 10:00:00 +05:00
					dataProcessor.GetBulk(metaObj.Name, "gt(created_time,10%3A00%3A00%20%2B05%3A00)", 1, callbackFunction, true)
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0]["id"]).To(Equal(record["id"]))
				})
			})

		})
	})

	It("can query records by multiple ids", func() {
		Context("having an object", func() {
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: true,
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			Context("and two records of this object", func() {
				recordOne, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "order1"}, auth.User{}, true)
				Expect(err).To(BeNil())
				recordTwo, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "order2"}, auth.User{}, true)
				Expect(err).To(BeNil())
				dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "order2"}, auth.User{}, true)

				matchedRecords := []map[string]interface{}{}
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}

				Context("query by date returns correct result", func() {
					query := fmt.Sprintf("in(id,(%d,%d))", int(recordOne["id"].(float64)), int(recordTwo["id"].(float64)))
					dataProcessor.GetBulk(metaObj.Name, query, 1, callbackFunction, true)
					Expect(matchedRecords).To(HaveLen(2))
					Expect(matchedRecords[0]["id"]).To(Equal(recordOne["id"]))
					Expect(matchedRecords[1]["id"]).To(Equal(recordTwo["id"]))
				})
			})

		})
	})

	It("can create record without specifying any value", func() {
		Context("having an object with optional fields", func() {
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "name",
						Type:     meta.FieldTypeString,
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

	It("can query with 'in' expression by single value", func() {
		Context("having an object", func() {
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			recordOne, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "order1"}, auth.User{}, true)
			Expect(err).To(BeNil())
			_, err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "order2"}, auth.User{}, true)
			Expect(err).To(BeNil())

			matchedRecords := []map[string]interface{}{}
			callbackFunction := func(obj map[string]interface{}) error {
				matchedRecords = append(matchedRecords, obj)
				return nil
			}
			Context("DataManager queries record with 'in' expression by single value", func() {
				query := fmt.Sprintf("in(id,(%d))", int(recordOne["id"].(float64)))
				dataProcessor.GetBulk(metaObj.Name, query, 1, callbackFunction, true)
				Expect(matchedRecords).To(HaveLen(1))
			})
		})
	})

	It("can create record with null value for optional field", func() {
		Context("having an object", func() {
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "name",
						Type:     meta.FieldTypeString,
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

	It("Performs case insensitive search when using 'like' operator", func() {

		Context("having an object with string field", func() {
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name: "name",
						Type: meta.FieldTypeString,
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			Context("and three records of this object", func() {

				By("two matching records")
				firstPersonRecord, _ := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "Some Person"}, auth.User{}, true)
				secondPersonRecord, _ := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "Some Another person"}, auth.User{}, true)

				By("and one mismatching record")
				dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "Some Other dog"}, auth.User{}, true)

				matchedRecords := []map[string]interface{}{}
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				Context("query by date returns correct result", func() {
					dataProcessor.GetBulk(metaObj.Name, "like(name,*Person*)", 1, callbackFunction, true)
					Expect(matchedRecords).To(HaveLen(2))
					Expect(matchedRecords[0]["id"]).To(Equal(firstPersonRecord["id"]))
					Expect(matchedRecords[1]["id"]).To(Equal(secondPersonRecord["id"]))
				})
			})

		})

	})
	It("Can create records containing reserved words", func() {
		Context("having an object named by reserved word and containing field named by reserved word", func() {
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name: "order",
						Type: meta.FieldTypeString,
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
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name: "name",
						Type: meta.FieldTypeString,
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

	It("returns a list of related outer links as a list of ids", func() {
		Context("having an object with outer link to another object", func() {
			orderMetaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
				},
			}
			orderMetaObj, err := metaStore.NewMeta(&orderMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(orderMetaObj)

			paymentMetaDescription := meta.MetaDescription{
				Name: "payment",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "order_id",
						Type:     meta.FieldTypeObject,
						LinkType: meta.LinkTypeInner,
						LinkMeta: "order",
						Optional: true,
					},
				},
			}
			paymentMetaObj, err := metaStore.NewMeta(&paymentMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(paymentMetaObj)

			orderMetaDescription = meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:           "payments",
						Type:           meta.FieldTypeArray,
						Optional:       true,
						LinkType:       meta.LinkTypeOuter,
						OuterLinkField: "order_id",
						LinkMeta:       "payment",
					},
				},
			}
			orderMetaObj, err = metaStore.NewMeta(&orderMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Update(orderMetaObj.Name, orderMetaObj, true, true)
			//

			Context("record can contain numeric value for string field", func() {
				record, err := dataProcessor.CreateRecord(orderMetaObj.Name, map[string]interface{}{}, auth.User{}, true)
				Expect(err).To(BeNil())
				record, err = dataProcessor.CreateRecord(paymentMetaObj.Name, map[string]interface{}{"order_id": record["id"]}, auth.User{}, true)
				Expect(err).To(BeNil())
				record, err = dataProcessor.CreateRecord(paymentMetaObj.Name, map[string]interface{}{"order_id": record["id"]}, auth.User{}, true)
				Expect(err).To(BeNil())

				matchedRecords := []map[string]interface{}{}
				callbackFunction := func(obj map[string]interface{}) error {
					matchedRecords = append(matchedRecords, obj)
					return nil
				}
				dataProcessor.GetBulk(orderMetaObj.Name, "", 1, callbackFunction, true)

				Expect(matchedRecords).To(HaveLen(1))
				payments, ok := matchedRecords[0]["payments"].([]interface{})
				Expect(ok).To(BeTrue())
				Expect(payments).To(HaveLen(2))
				paymentId, ok := payments[0].(string)
				Expect(ok).To(BeTrue())
				Expect(paymentId).To(Equal("1"))

			})
		})
	})
})
