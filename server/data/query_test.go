package data_test

import (
	"custodian/server/auth"
	"custodian/server/data"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/pg"
	"custodian/server/pg/migrations/operations/object"
	"custodian/server/transactions"
	"custodian/utils"
	"fmt"
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := pg.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(dbTransactionManager)
	metaDescriptionSyncer := pg.NewPgMetaDescriptionSyncer(globalTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	testObjEnumName := utils.RandomString(8)

	It("can query records by date field", func() {
		Context("having an object with date field", func() {
			metaDescription := description.MetaDescription{
				Name: utils.RandomString(8),
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
						Name: "date",
						Type: description.FieldTypeDate,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			Context("and two records with dates that differ by a week", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"date": "2018-05-29"}, auth.User{})
				dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"date": "2018-05-22"}, auth.User{})
				Expect(err).To(BeNil())
				Context("query by date returns correct result", func() {
					_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "gt(date,2018-05-23)", nil, nil, 1, false)
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0].Data["id"]).To(Equal(record.Data["id"]))
				})
			})

		})
	})

	It("can query records by string PK value", func() {
		Context("having an A object with string PK field", func() {
			testObjAName := utils.RandomString(8)
			testObjBName := utils.RandomString(8)

			metaDescription := description.MetaDescription{
				Name: testObjAName,
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			By("having two records of this object")

			_, err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"id": "PKVALUE"}, auth.User{})
			Expect(err).To(BeNil())

			_, err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"id": "ANOTHERPKVALUE"}, auth.User{})
			Expect(err).To(BeNil())

			By("having another object, containing A object as a link")

			metaDescription = description.MetaDescription{
				Name: testObjBName,
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeString,
						Optional: false,
					},
					{
						Name:     testObjAName,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjAName,
						Optional: true,
					},
				},
			}
			bMetaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())

			By("having a record of B object")
			_, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"id": "id", testObjAName: "PKVALUE"}, auth.User{})
			Expect(err).To(BeNil())

			Context("query by PK returns correct result", func() {
				_, matchedRecords, _ := dataProcessor.GetBulk(bMetaObj.Name, fmt.Sprintf("eq(%s,PKVALUE)", testObjAName), nil, nil, 1, false)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data["id"]).To(Equal("id"))
			})

		})
	})

	It("can query records by datetime field", func() {
		Context("having an object with datetime field", func() {
			metaDescription := description.MetaDescription{
				Name: utils.RandomString(8),
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
						Name: "created",
						Type: description.FieldTypeDateTime,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			Context("and two records with 'created' values that differ by a week", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"created": "2018-05-29T15:29:58.627755+05:00"}, auth.User{})
				Expect(err).To(BeNil())
				dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"created": "2018-05-22T15:29:58.627755+05:00"}, auth.User{})
				Context("query by 'created' field returns correct result", func() {
					_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "gt(created,2018-05-23)", nil, nil, 1, false)
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0].Data["id"]).To(Equal(record.Data["id"]))
				})
			})

		})
	})

	It("can query records by time field", func() {
		Context("having an object with datetime field", func() {
			metaDescription := description.MetaDescription{
				Name: utils.RandomString(8),
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
						Name: "created_time",
						Type: description.FieldTypeTime,
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			Context("and two records with 'created_time' values that differ by several hours", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"created_time": "14:00:00"}, auth.User{})
				dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"created_time": "09:00:00"}, auth.User{})
				Expect(err).To(BeNil())
				Context("query by 'created' field returns correct result", func() {
					//query by value greater than 10:00:00 +05:00
					_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "gt(created_time,10:00:00)", nil, nil, 1, false)
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0].Data["id"]).To(Equal(record.Data["id"]))
				})
			})

		})
	})

	It("can query records by multiple ids", func() {
		Context("having an object", func() {
			metaDescription := description.MetaDescription{
				Name: utils.RandomString(8),
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
						Name:     "name",
						Type:     description.FieldTypeString,
						Optional: true,
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			Context("and two records of this object", func() {
				recordOne, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "order1"}, auth.User{})
				Expect(err).To(BeNil())
				recordTwo, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "order2"}, auth.User{})
				Expect(err).To(BeNil())
				dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "order2"}, auth.User{})

				Context("query by date returns correct result", func() {
					query := fmt.Sprintf("in(id,(%d,%d))", int(recordOne.Data["id"].(float64)), int(recordTwo.Data["id"].(float64)))
					_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, query, nil, nil, 1, false)
					Expect(matchedRecords).To(HaveLen(2))
					Expect(matchedRecords[0].Data["id"]).To(Equal(recordOne.Data["id"]))
					Expect(matchedRecords[1].Data["id"]).To(Equal(recordTwo.Data["id"]))
				})
			})

		})
	})

	It("can query with 'in' expression by single value", func() {
		Context("having an object", func() {
			metaDescription := description.MetaDescription{
				Name: utils.RandomString(8),
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
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			recordOne, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "order1"}, auth.User{})
			Expect(err).To(BeNil())
			_, err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "order2"}, auth.User{})
			Expect(err).To(BeNil())

			Context("DataManager queries record with 'in' expression by single value", func() {
				query := fmt.Sprintf("in(id,(%d))", int(recordOne.Data["id"].(float64)))
				_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, query, nil, nil, 1, false)
				Expect(matchedRecords).To(HaveLen(1))
			})
		})
	})

	It("Performs case insensitive search when using 'like' operator", func() {

		Context("having an object with string field", func() {
			metaDescription := description.MetaDescription{
				Name: utils.RandomString(8),
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
						Name: "name",
						Type: description.FieldTypeString,
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			Context("and three records of this object", func() {

				By("two matching records")
				firstPersonRecord, _ := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "Some Person"}, auth.User{})
				secondPersonRecord, _ := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "Some Another person"}, auth.User{})

				By("and one mismatching record")
				dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "Some Other dog"}, auth.User{})

				Context("query by date returns correct result", func() {
					_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "like(name,*Person*)", nil, nil, 1, false)
					Expect(matchedRecords).To(HaveLen(2))
					Expect(matchedRecords[0].Data["id"]).To(Equal(firstPersonRecord.Data["id"]))
					Expect(matchedRecords[1].Data["id"]).To(Equal(secondPersonRecord.Data["id"]))
				})
			})

		})

	})

	It("returns a list of related outer links as a list of ids", func() {
		Context("having an object with outer link to another object", func() {
			orderMetaDescription := description.MetaDescription{
				Name: "test_order",
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
			orderMetaObj, err := metaStore.NewMeta(&orderMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(orderMetaObj)

			paymentMetaDescription := description.MetaDescription{
				Name: "test_payment",
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
						Name:     "order_id",
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: "test_order",
						Optional: true,
					},
				},
			}
			paymentMetaObj, err := metaStore.NewMeta(&paymentMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(paymentMetaObj)

			orderMetaDescription = description.MetaDescription{
				Name: "test_order",
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
						Name:           "payments",
						Type:           description.FieldTypeArray,
						Optional:       true,
						LinkType:       description.LinkTypeOuter,
						OuterLinkField: "order_id",
						LinkMeta:       "test_payment",
					},
				},
			}
			orderMetaObj, err = metaStore.NewMeta(&orderMetaDescription)
			(&description.NormalizationService{}).Normalize(&orderMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Update(orderMetaObj.Name, orderMetaObj, true, true)
			//

			Context("record can contain numeric value for string field", func() {
				record, err := dataProcessor.CreateRecord(orderMetaObj.Name, map[string]interface{}{}, auth.User{})
				Expect(err).To(BeNil())
				record, err = dataProcessor.CreateRecord(paymentMetaObj.Name, map[string]interface{}{"order_id": record.Data["id"]}, auth.User{})
				Expect(err).To(BeNil())
				record, err = dataProcessor.CreateRecord(paymentMetaObj.Name, map[string]interface{}{"order_id": record.Data["id"]}, auth.User{})
				Expect(err).To(BeNil())

				_, matchedRecords, _ := dataProcessor.GetBulk(orderMetaObj.Name, "", nil, nil, 1, false)

				Expect(matchedRecords).To(HaveLen(1))
				payments, ok := matchedRecords[0].Data["payments"].([]interface{})
				Expect(ok).To(BeTrue())
				Expect(payments).To(HaveLen(2))
				paymentId, ok := payments[0].(float64)
				Expect(ok).To(BeTrue())
				Expect(paymentId).To(Equal(float64(1)))
			})
		})
	})

	It("can query records by related record`s attribute", func() {
		Context("having an object A", func() {
			testObjAName := utils.RandomString(8)
			testObjBName := utils.RandomString(8)

			aMetaDescription := description.MetaDescription{
				Name: testObjAName,
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
						Name:     "name",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
			}
			aMetaObj, _ := metaStore.NewMeta(&aMetaDescription)
			metaStore.Create(aMetaObj)

			bMetaDescription := description.MetaDescription{
				Name: testObjBName,
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
						Name:     testObjAName,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjAName,
					},
				},
			}
			bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())

			By("having two records of object A")

			aRecordOne, err := dataProcessor.CreateRecord(aMetaDescription.Name, map[string]interface{}{"name": "ARecordOne"}, auth.User{})
			Expect(err).To(BeNil())

			aRecordTwo, err := dataProcessor.CreateRecord(aMetaDescription.Name, map[string]interface{}{"name": "ARecordTwo"}, auth.User{})
			Expect(err).To(BeNil())

			By("and two records of object B, each has link to object A")

			bRecordOne, err := dataProcessor.CreateRecord(bMetaDescription.Name, map[string]interface{}{testObjAName: aRecordOne.Data["id"]}, auth.User{})
			Expect(err).To(BeNil())

			_, err = dataProcessor.CreateRecord(bMetaDescription.Name, map[string]interface{}{testObjAName: aRecordTwo.Data["id"]}, auth.User{})
			Expect(err).To(BeNil())

			Context("query by a`s attribute returns correct result", func() {
				query := fmt.Sprintf("eq(%s.name,%s)", testObjAName, aRecordOne.Data["name"])
				_, matchedRecords, err := dataProcessor.GetBulk(bMetaObj.Name, query, nil, nil, 1, false)
				Expect(err).To(BeNil())
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data["id"]).To(Equal(bRecordOne.Data["id"]))
			})

		})
	})

	It("can retrieve records with null inner link value", func() {
		Context("having an object A", func() {
			testObjAName := utils.RandomString(8)
			testObjBName := utils.RandomString(8)

			aMetaDescription := description.MetaDescription{
				Name: testObjAName,
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
						Name:     "name",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
			}
			aMetaObj, _ := metaStore.NewMeta(&aMetaDescription)
			metaStore.Create(aMetaObj)

			bMetaDescription := description.MetaDescription{
				Name: testObjBName,
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
						Name:     testObjAName,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjAName,
						Optional: true,
					},
				},
			}
			bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())

			By("having a record of object B with null a value")

			bRecordOne, err := dataProcessor.CreateRecord(bMetaDescription.Name, map[string]interface{}{"name": "B record"}, auth.User{})
			Expect(err).To(BeNil())

			Context("query by a`s attribute returns correct result", func() {
				query := fmt.Sprintf("eq(id,%s)", strconv.Itoa(int(bRecordOne.Data["id"].(float64))))
				_, matchedRecords, err := dataProcessor.GetBulk(bMetaObj.Name, query, nil, nil, 1, false)
				Expect(err).To(BeNil())
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data).To(HaveKey(testObjAName))
				Expect(matchedRecords[0].Data[testObjAName]).To(BeNil())
			})
		})
	})

	It("can query through 3 related objects", func() {
		Context("having an object with outer link to another object", func() {
			testObjAName := utils.RandomString(8)
			testObjBName := utils.RandomString(8)
			testObjCName := utils.RandomString(8)

			aMetaDescription := description.MetaDescription{
				Name: testObjAName,
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
						Name: "name",
						Type: description.FieldTypeString,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMetaObj)

			bMetaDescription := description.MetaDescription{
				Name: testObjBName,
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
						Name:     testObjAName,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjAName,
						Optional: false,
					},
				},
			}
			bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMetaObj)

			cMetaDescription := description.MetaDescription{
				Name: testObjCName,
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
						Name:     testObjBName,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjBName,
						Optional: false,
					},
				},
			}
			cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(cMetaObj)

			dMetaDescription := description.MetaDescription{
				Name: "d",
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
						Name:     testObjCName,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjCName,
						Optional: false,
					},
				},
			}
			dMetaObj, err := metaStore.NewMeta(&dMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(dMetaObj)

			aRecord, err := dataProcessor.CreateRecord(
				aMetaDescription.Name,
				map[string]interface{}{"name": "Arecord"},
				auth.User{},
			)
			Expect(err).To(BeNil())

			bRecord, err := dataProcessor.CreateRecord(
				bMetaDescription.Name,
				map[string]interface{}{testObjAName: aRecord.Data["id"]},
				auth.User{},
			)
			Expect(err).To(BeNil())

			cRecord, err := dataProcessor.CreateRecord(
				cMetaDescription.Name,
				map[string]interface{}{testObjBName: bRecord.Data["id"]},
				auth.User{},
			)
			Expect(err).To(BeNil())

			dRecord, err := dataProcessor.CreateRecord(
				dMetaDescription.Name,
				map[string]interface{}{testObjCName: cRecord.Data["id"]},
				auth.User{},
			)
			Expect(err).To(BeNil())

			Context("query by date returns correct result", func() {
				_, matchedRecords, err := dataProcessor.GetBulk(
					dMetaDescription.Name,
					fmt.Sprintf("eq(%s.%s.%s.name,%s)", testObjCName, testObjBName, testObjAName, "Arecord"), nil, nil,
					1,
					false,
				)
				Expect(err).To(BeNil())
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data["id"]).To(Equal(dRecord.Data["id"]))
			})
		})
	})

	It("can query through 1 generic and 2 related objects", func() {
		testObjAName := utils.RandomString(8)
		testObjBName := utils.RandomString(8)
		testObjCName := utils.RandomString(8)

		aMetaDescription := description.MetaDescription{
			Name: testObjAName,
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
					Name: "name",
					Type: description.FieldTypeString,
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		bMetaDescription := description.MetaDescription{
			Name: testObjBName,
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
					Name:     testObjAName,
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjAName,
					Optional: false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(bMetaObj)

		cMetaDescription := description.MetaDescription{
			Name: testObjCName,
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
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjBName},
					Optional:     false,
				},
			},
		}
		cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(cMetaObj)

		aRecord, err := dataProcessor.CreateRecord(
			aMetaDescription.Name,
			map[string]interface{}{"name": "Arecord"},
			auth.User{},
		)
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(
			bMetaDescription.Name,
			map[string]interface{}{testObjAName: aRecord.Data["id"]},
			auth.User{},
		)
		Expect(err).To(BeNil())

		cRecord, err := dataProcessor.CreateRecord(
			cMetaDescription.Name,
			map[string]interface{}{"target_object": map[string]interface{}{"_object": bMetaObj.Name, "id": bRecord.Data["id"]}},
			auth.User{},
		)
		Expect(err).To(BeNil())

		Context("query by date returns correct result", func() {
			_, matchedRecords, err := dataProcessor.GetBulk(
				cMetaDescription.Name,
				fmt.Sprintf("eq(target_object.%s.%s.name,%s)", testObjBName, testObjAName, "Arecord"), nil, nil,
				1,
				false,
			)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
			Expect(matchedRecords[0].Data["id"]).To(Equal(cRecord.Data["id"]))
		})
	})

	It("always uses additional ordering by primary key", func() {
		testObjAName := utils.RandomString(8)

		aMetaDescription := description.MetaDescription{
			Name: testObjAName,
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
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		_, err = dataProcessor.CreateRecord(
			aMetaDescription.Name,
			map[string]interface{}{"name": "Arecord"},
			auth.User{},
		)
		Expect(err).To(BeNil())

		aNilNameFirst, err := dataProcessor.CreateRecord(
			aMetaDescription.Name,
			map[string]interface{}{"name": nil},
			auth.User{},
		)
		Expect(err).To(BeNil())

		aNilNameSecond, err := dataProcessor.CreateRecord(
			aMetaDescription.Name,
			map[string]interface{}{"name": nil},
			auth.User{},
		)
		Expect(err).To(BeNil())

		Context("query by date returns correct result", func() {
			_, records, err := dataProcessor.GetBulk(
				aMetaDescription.Name,
				"sort(name)", nil, nil,
				1,
				false,
			)
			Expect(err).To(BeNil())
			Expect(records).To(HaveLen(3))
			Expect(records[1].Data["id"]).To(Equal(aNilNameFirst.Data["id"]))
			Expect(records[2].Data["id"]).To(Equal(aNilNameSecond.Data["id"]))
		})
	})

	It("omits outer links if omit_outers flag specified", func() {
		Context("having an object with outer link to another object", func() {
			orderMetaDescription := description.MetaDescription{
				Name: "test_order",
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
			orderMetaObj, err := metaStore.NewMeta(&orderMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(orderMetaObj)

			paymentMetaDescription := description.MetaDescription{
				Name: "test_payment",
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
						Name:     "order_id",
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: "test_order",
						Optional: true,
					},
				},
			}
			paymentMetaObj, err := metaStore.NewMeta(&paymentMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(paymentMetaObj)

			orderMetaDescription = description.MetaDescription{
				Name: "test_order",
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
						Name:           "payments",
						Type:           description.FieldTypeArray,
						Optional:       true,
						LinkType:       description.LinkTypeOuter,
						OuterLinkField: "order_id",
						LinkMeta:       "test_payment",
					},
				},
			}
			orderMetaObj, err = metaStore.NewMeta(&orderMetaDescription)
			(&description.NormalizationService{}).Normalize(&orderMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Update(orderMetaObj.Name, orderMetaObj, true, true)
			//

			_, err = dataProcessor.CreateRecord(orderMetaObj.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			_, matchedRecords, _ := dataProcessor.GetBulk(orderMetaObj.Name, "", nil, nil, 1, true)

			Expect(matchedRecords).To(HaveLen(1))
			Expect(matchedRecords[0].Data).NotTo(HaveKey("payments"))
		})
	})

	It("can query by 'Objects' field values", func() {
		Context("having an object with outer link to another object", func() {
			testObjAName := utils.RandomString(8)
			testObjBName := utils.RandomString(8)

			aMetaDescription := description.MetaDescription{
				Name: testObjAName,
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
						Name: "name",
						Type: description.FieldTypeString,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())

			bMetaDescription := description.MetaDescription{
				Name: testObjBName,
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
						Name:     "as",
						Type:     description.FieldTypeObjects,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjAName,
						Optional: true,
					},
				},
			}
			bMetaObject, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMetaObject)

			aRecordName := "Some-A-record"
			//create B record with A record
			bRecord, err := dataProcessor.CreateRecord(
				bMetaObject.Name,
				map[string]interface{}{
					"as": []interface{}{
						map[string]interface{}{
							"name": aRecordName,
						},
					},
				},
				auth.User{},
			)
			Expect(err).To(BeNil())

			//create B record which should not be in query result
			_, err = dataProcessor.CreateRecord(
				bMetaObject.Name,
				map[string]interface{}{},
				auth.User{},
			)
			Expect(err).To(BeNil())

			_, matchedRecords, err := dataProcessor.GetBulk(bMetaObject.Name, fmt.Sprintf("eq(as.name,%s)", aRecordName), nil, nil, 2, true)
			Expect(err).To(BeNil())

			Expect(matchedRecords).To(HaveLen(1))
			Expect(matchedRecords[0].Data["id"]).To(Equal(bRecord.Pk()))
		})
	})
	It("can make query with special symbol", func() {
		Context("having an object with special symbol", func() {
			aMetaDescription := description.MetaDescription{
				Name: "a_ijisl2",
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
						Name: "name",
						Type: description.FieldTypeString,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())

			testRecordName := "H&M"

			aRecordName, err := dataProcessor.CreateRecord(aMetaDescription.Name, map[string]interface{}{"name": "H&M"}, auth.User{})
			Expect(err).To(BeNil())

			_, matchedRecords, err := dataProcessor.GetBulk(aMetaDescription.Name, "eq(name,H\\&M)", nil, nil, 2, true)
			Expect(err).To(BeNil())

			Expect(matchedRecords).To(HaveLen(1))
			Expect(matchedRecords[0].Data["id"]).To(Equal(aRecordName.Pk()))
			Expect(matchedRecords[0].Data["name"]).To(Equal(testRecordName))

			testBackslashRecordName := "H\\M"

			backslahRecord, err := dataProcessor.CreateRecord(aMetaDescription.Name, map[string]interface{}{"name": "H\\M"}, auth.User{})
			Expect(err).To(BeNil())

			_, matchedRecords, err = dataProcessor.GetBulk(aMetaDescription.Name, "eq(name,H\\\\M)", nil, nil, 2, true)
			Expect(err).To(BeNil())

			Expect(matchedRecords).To(HaveLen(1))
			Expect(matchedRecords[0].Data["id"]).To(Equal(backslahRecord.Pk()))
			Expect(matchedRecords[0].Data["name"]).To(Equal(testBackslashRecordName))
		})
	})
	It("can filter by enum field values ", func() {
		Context("having an object with datetime field", func() {
			metaDescription := &description.MetaDescription{
				Name: testObjEnumName,
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
						Name:     "nameEnum",
						Type:     description.FieldTypeEnum,
						Optional: true,
						Enum:     []string{"Val1", "Val2"},
					},
				},
			}
			//create MetaDescription
			operation := object.NewCreateObjectOperation(metaDescription)
			//sync MetaDescription

			metaDescription, err := operation.SyncMetaDescription(nil, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())
			err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
			globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())
			//

			Context("having two records with different enum values", func() {
				testEnumValue := "Val1"

				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"nameEnum": testEnumValue}, auth.User{})
				Expect(err).To(BeNil())
				_, err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"nameEnum": "Val2"}, auth.User{})
				Expect(err).To(BeNil())
				Context("query by 'nameEnum' field returns correct result", func() {
					_, matchedRecords, err := dataProcessor.GetBulk(metaDescription.Name, "eq(nameEnum,Val1)", nil, nil, 1, false)
					Expect(err).To(BeNil())
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0].Data["id"]).To(Equal(record.Data["id"]))
					Expect(matchedRecords[0].Data["nameEnum"]).To(Equal("Val1"))
				})
			})
		})
	})

	It("can query by camelCase string ", func() {
		Context("having an object with datetime field", func() {
			metaDescription := &description.MetaDescription{
				Name: testObjEnumName,
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
						Name:     "stringCamelCase",
						Type:     description.FieldTypeString,
						Optional: true,
					},
				},
			}
			//create MetaDescription
			operation := object.NewCreateObjectOperation(metaDescription)
			//sync MetaDescription
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())
			metaDescription, err = operation.SyncMetaDescription(nil, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
			globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())
			//

			Context("creating record in camelCaseField, filter it ", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"stringCamelCase": "Camel"}, auth.User{})
				Expect(err).To(BeNil())
				_, err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"stringCamelCase": "NotCamel"}, auth.User{})
				Expect(err).To(BeNil())
				Context("query by 'name_enum' field returns correct result", func() {
					_, matchedRecords, err := dataProcessor.GetBulk(metaDescription.Name, "eq(stringCamelCase,Camel)", nil, nil, 1, false)
					Expect(err).To(BeNil())
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0].Data["id"]).To(Equal(record.Data["id"]))
					Expect(matchedRecords[0].Data["stringCamelCase"]).To(Equal("Camel"))
				})
			})
		})
	})
})
