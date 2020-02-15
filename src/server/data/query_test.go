package data_test

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/auth"
	"server/data"
	"server/object"
	"server/object/driver"
	"server/object/meta"

	"server/pg"
	"server/pg/transactions"
	"strconv"
	"utils"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := transactions.NewPgDbTransactionManager(dataManager)

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := object.NewStore(driver)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		metaStore.Flush()
	})

	It("can query records by date field", func() {
		Context("having an object with date field", func() {
			metaDescription := object.GetBaseMetaData(utils.RandomString(8))
			metaDescription.AddField(&meta.Field{
				Name: "date",
				Type: meta.FieldTypeDate,
			})
			metaObj, err := metaStore.NewMeta(metaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(metaObj)

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
			metaDescription := object.GetBaseMetaData(utils.RandomString(8))
			metaObj, err := metaStore.NewMeta(metaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(metaObj)

			By("having two records of this object")

			_, err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"id": "PKVALUE"}, auth.User{})
			Expect(err).To(BeNil())

			_, err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"id": "ANOTHERPKVALUE"}, auth.User{})
			Expect(err).To(BeNil())

			By("having another object, containing A object as a link")

			metaDescription = object.GetBaseMetaData(utils.RandomString(8))
			metaDescription.AddField(&meta.Field{
				Name:     "a",
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				LinkMeta: metaObj,
				Optional: true,
			})
			bMetaObj, err := metaStore.NewMeta(metaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMetaObj)

			By("having a record of B object")
			_, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"id": "id", "a": "PKVALUE"}, auth.User{})
			Expect(err).To(BeNil())

			Context("query by PK returns correct result", func() {
				_, matchedRecords, _ := dataProcessor.GetBulk(bMetaObj.Name, "eq(a,PKVALUE)", nil, nil, 1, false)
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data["id"]).To(Equal("id"))
			})

		})
	})

	It("can query records by datetime field", func() {
		Context("having an object with datetime field", func() {
			metaDescription := object.GetBaseMetaData(utils.RandomString(8))
			metaDescription.AddField(&meta.Field{Name: "created", Type: meta.FieldTypeDateTime})
			metaObj, err := metaStore.NewMeta(metaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(metaObj)

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
			metaDescription := object.GetBaseMetaData(utils.RandomString(8))
			metaDescription.AddField(&meta.Field{Name: "created_time", Type: meta.FieldTypeTime})
			metaObj, _ := metaStore.NewMeta(metaDescription)
			metaStore.Create(metaObj)

			Context("and two records with 'created_time' values that differ by several hours", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"created_time": "14:00:00 +05:00"}, auth.User{})
				dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"created_time": "09:00:00 +05:00"}, auth.User{})
				Expect(err).To(BeNil())
				Context("query by 'created' field returns correct result", func() {
					//query by value greater than 10:00:00 +05:00
					_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "gt(created_time,10%3A00%3A00%20%2B05%3A00)", nil, nil, 1, false)
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0].Data["id"]).To(Equal(record.Data["id"]))
				})
			})

		})
	})

	It("can query records by multiple ids", func() {
		Context("having an object", func() {
			metaDescription := object.GetBaseMetaData(utils.RandomString(8))
			metaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString, Optional: true})
			metaObj, _ := metaStore.NewMeta(metaDescription)
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
			metaDescription := object.GetBaseMetaData(utils.RandomString(8))
			metaObj, _ := metaStore.NewMeta(metaDescription)
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
			metaDescription := object.GetBaseMetaData(utils.RandomString(8))
			metaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString})
			metaObj, _ := metaStore.NewMeta(metaDescription)
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
			orderMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			orderMetaObj, err := metaStore.NewMeta(orderMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(orderMetaObj)

			paymentMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			paymentMetaDescription.AddField(&meta.Field{
				Name:     "order_id",
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				LinkMeta: orderMetaObj,
				Optional: true,
			})
			paymentMetaObj, err := metaStore.NewMeta(paymentMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(paymentMetaObj)

			orderMetaDescription.AddField(&meta.Field{
				Name:           "payments",
				Type:           meta.FieldTypeArray,
				Optional:       true,
				LinkType:       meta.LinkTypeOuter,
				OuterLinkField: paymentMetaObj.FindField("order_id"),
				LinkMeta:       paymentMetaObj,
			})
			orderMetaObj, err = metaStore.NewMeta(orderMetaDescription)
			(&meta.NormalizationService{}).Normalize(orderMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Update(orderMetaObj)

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
			aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			aMetaDescription.AddField(&meta.Field{Name:"name", Type:     meta.FieldTypeString, Optional: false})
			aMetaObj, _ := metaStore.NewMeta(aMetaDescription)
			metaStore.Create(aMetaObj)

			bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField(&meta.Field{
				Name:     "a",
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				LinkMeta: aMetaObj,
			})
			bMetaObj, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMetaObj)

			By("having two records of object A")

			aRecordOne, err := dataProcessor.CreateRecord(aMetaDescription.Name, map[string]interface{}{"name": "ARecordOne"}, auth.User{})
			Expect(err).To(BeNil())

			aRecordTwo, err := dataProcessor.CreateRecord(aMetaDescription.Name, map[string]interface{}{"name": "ARecordTwo"}, auth.User{})
			Expect(err).To(BeNil())

			By("and two records of object B, each has link to object A")

			bRecordOne, err := dataProcessor.CreateRecord(bMetaDescription.Name, map[string]interface{}{"a": aRecordOne.Data["id"]}, auth.User{})
			Expect(err).To(BeNil())

			_, err = dataProcessor.CreateRecord(bMetaDescription.Name, map[string]interface{}{"a": aRecordTwo.Data["id"]}, auth.User{})
			Expect(err).To(BeNil())

			Context("query by a`s attribute returns correct result", func() {
				query := fmt.Sprintf("eq(a.name,%s)", aRecordOne.Data["name"])
				_, matchedRecords, err := dataProcessor.GetBulk(bMetaObj.Name, query, nil, nil, 1, false)
				Expect(err).To(BeNil())
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data["id"]).To(Equal(bRecordOne.Data["id"]))
			})

		})
	})

	It("can retrieve records with null inner link value", func() {
		Context("having an object A", func() {
			aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			aMetaDescription.AddField(&meta.Field{Name:"name", Type: meta.FieldTypeString, Optional: false})
			aMetaObj, _ := metaStore.NewMeta(aMetaDescription)
			metaStore.Create(aMetaObj)

			bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField(&meta.Field{
				Name:     "a",
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				LinkMeta: aMetaObj,
				Optional: true,
			})
			bMetaObj, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMetaObj)

			By("having a record of object B with null a value")

			bRecordOne, err := dataProcessor.CreateRecord(bMetaDescription.Name, map[string]interface{}{"name": "B record"}, auth.User{})
			Expect(err).To(BeNil())

			Context("query by a`s attribute returns correct result", func() {
				query := fmt.Sprintf("eq(id,%s)", strconv.Itoa(int(bRecordOne.Data["id"].(float64))))
				_, matchedRecords, err := dataProcessor.GetBulk(bMetaObj.Name, query, nil, nil, 1, false)
				Expect(err).To(BeNil())
				Expect(matchedRecords).To(HaveLen(1))
				Expect(matchedRecords[0].Data).To(HaveKey("a"))
				Expect(matchedRecords[0].Data["a"]).To(BeNil())
			})
		})
	})

	It("can query through 3 related objects", func() {
		Context("having an object with outer link to another object", func() {
			aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			aMetaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString})
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMetaObj)

			bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField(&meta.Field{
				Name:     "a",
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				LinkMeta: aMetaObj,
				Optional: false,
			})
			bMetaObj, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMetaObj)

			cMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			cMetaDescription.AddField(&meta.Field{
				Name:     "b",
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				LinkMeta: bMetaObj,
				Optional: false,
			})
			cMetaObj, err := metaStore.NewMeta(cMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(cMetaObj)

			dMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			dMetaDescription.AddField(&meta.Field{
				Name:     "c",
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				LinkMeta: cMetaObj,
				Optional: false,
			})
			dMetaObj, err := metaStore.NewMeta(dMetaDescription)
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
				map[string]interface{}{"a": aRecord.Data["id"]},
				auth.User{},
			)
			Expect(err).To(BeNil())

			cRecord, err := dataProcessor.CreateRecord(
				cMetaDescription.Name,
				map[string]interface{}{"b": bRecord.Data["id"]},
				auth.User{},
			)
			Expect(err).To(BeNil())

			dRecord, err := dataProcessor.CreateRecord(
				dMetaDescription.Name,
				map[string]interface{}{"c": cRecord.Data["id"]},
				auth.User{},
			)
			Expect(err).To(BeNil())

			Context("query by date returns correct result", func() {
				_, matchedRecords, err := dataProcessor.GetBulk(
					dMetaDescription.Name,
					fmt.Sprintf("eq(c.b.a.name,%s)", "Arecord"), nil, nil,
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
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		aMetaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString})
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.AddField(&meta.Field{
			Name:     "a",
			Type:     meta.FieldTypeObject,
			LinkType: meta.LinkTypeInner,
			LinkMeta: aMetaObj,
			Optional: false,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(bMetaObj)

		cMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.AddField(&meta.Field{
			Name:         "target_object",
			Type:         meta.FieldTypeGeneric,
			LinkType:     meta.LinkTypeInner,
			LinkMetaList: []*meta.Meta{bMetaObj},
			Optional:     false,
		})
		cMetaObj, err := metaStore.NewMeta(cMetaDescription)
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
			map[string]interface{}{"a": aRecord.Data["id"]},
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
				fmt.Sprintf("eq(target_object.b.a.name,%s)", "Arecord"), nil, nil,
				1,
				false,
			)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
			Expect(matchedRecords[0].Data["id"]).To(Equal(cRecord.Data["id"]))
		})
	})

	It("always uses additional ordering by primary key", func() {
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		aMetaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString, Optional: true})
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
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
				"sort(+name)", nil, nil,
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
			orderMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			orderMetaObj, err := metaStore.NewMeta(orderMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(orderMetaObj)

			paymentMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			paymentMetaDescription.AddField(&meta.Field{
				Name:     "order_id",
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				LinkMeta: orderMetaObj,
				Optional: true,
			})
			paymentMetaObj, err := metaStore.NewMeta(paymentMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(paymentMetaObj)

			orderMetaDescription = object.GetBaseMetaData(utils.RandomString(8))
			orderMetaDescription.AddField(&meta.Field{
				Name:           "payments",
				Type:           meta.FieldTypeArray,
				Optional:       true,
				LinkType:       meta.LinkTypeOuter,
				OuterLinkField: paymentMetaObj.FindField("order_id"),
				LinkMeta:       paymentMetaObj,
			})
			orderMetaObj, err = metaStore.NewMeta(orderMetaDescription)
			(&meta.NormalizationService{}).Normalize(orderMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Update(orderMetaObj)
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
			aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			aMetaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString})
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMetaObj)

			bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField(&meta.Field{
				Name:     "as",
				Type:     meta.FieldTypeObjects,
				LinkType: meta.LinkTypeInner,
				LinkMeta: aMetaObj,
				Optional: true,
			})
			bMetaObject, err := metaStore.NewMeta(bMetaDescription)
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
})
