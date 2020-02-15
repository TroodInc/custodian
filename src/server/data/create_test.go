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
	"utils"
)

var _ = Describe("Create test", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	dbTransactionManager := transactions.NewPgDbTransactionManager(dataManager)

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := object.NewStore(driver)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		//metaStore.Flush()
	})

	havingObjectA := func() *meta.Meta {
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		aMetaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString, Optional: true})


		(&meta.NormalizationService{}).Normalize(aMetaDescription)


		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		return metaStore.Create(aMetaObj)
	}

	havingObjectB := func(A *meta.Meta) *meta.Meta {
		bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.AddField(&meta.Field{
				Name:     "a",
				Type:     meta.FieldTypeObject,
				Optional: false,
				LinkType: meta.LinkTypeInner,
				LinkMeta: A,
				OnDelete: meta.OnDeleteCascade.ToVerbose(),
			},
			&meta.Field{
				Name:     "name",
				Type:     meta.FieldTypeString,
				Optional: true,
			},
		)
		(&meta.NormalizationService{}).Normalize(bMetaDescription)
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		return metaStore.Create(bMetaObj)
	}

	havingObjectC := func() *meta.Meta {
		cMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.AddField(&meta.Field{
			Name:     "name",
			Type:     meta.FieldTypeString,
			Optional: true,
		})
		(&meta.NormalizationService{}).Normalize(cMetaDescription)
		cMetaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		return metaStore.Create(cMetaObj)
	}

	havingObjectAWithManuallySetOuterLinkToB := func(A, B *meta.Meta) *meta.Meta {
		A.AddField(
			&meta.Field{Name: "name", Type: meta.FieldTypeString, Optional: true},
			&meta.Field{
				Name:           "b_set",
				Type:           meta.FieldTypeArray,
				LinkType:       meta.LinkTypeOuter,
				OuterLinkField: B.FindField("a"),
				LinkMeta:       B,
				Optional:       true,
			},
		)
		return metaStore.Update(A)
	}

	havingObjectAWithObjectsLinkToB := func(A, C *meta.Meta) *meta.Meta {
		A.AddField(
			&meta.Field{Name: "name", Type: meta.FieldTypeString,Optional: true},
			&meta.Field{Name: "cs", Type: meta.FieldTypeObjects, LinkType: meta.LinkTypeInner, LinkMeta: C},
		)
		return metaStore.Update(A)
	}

	It("can create a record containing null value of foreign key field", func() {

		Context("having Reason object", func() {
			reasonMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			reasonMetaObj, err := metaStore.NewMeta(reasonMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(reasonMetaObj)

			Context("and Lead object referencing Reason object", func() {
				leadMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
				leadMetaDescription.AddField(
						&meta.Field{Name: "name", Type: meta.FieldTypeString},
						&meta.Field{
							Name:     "decline_reason",
							Type:     meta.FieldTypeObject,
							Optional: true,
							LinkType: meta.LinkTypeInner,
							LinkMeta: reasonMetaObj,
						},
				)
				leadMetaObj, err := metaStore.NewMeta(leadMetaDescription)
				Expect(err).To(BeNil())
				metaStore.Create(leadMetaObj)
				Context("Lead record with empty reason is created", func() {
					leadData := map[string]interface{}{
						"name": "newLead",
					}
					user := auth.User{}
					record, err := dataProcessor.CreateRecord(leadMetaDescription.Name, leadData, user)
					Expect(err).To(BeNil())
					Expect(record.Data).To(HaveKey("decline_reason"))
				})
			})
		})
	})

	It("can create record without specifying any value", func() {
		Context("having an object with optional fields", func() {
			metaDescription :=  object.GetBaseMetaData(utils.RandomString(8))
			metaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString, Optional: true})
			metaObj, _ := metaStore.NewMeta(metaDescription)
			metaStore.Create(metaObj)

			Context("DataManager creates record without any values", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{}, auth.User{})
				Expect(err).To(BeNil())
				Expect(record.Data["id"]).To(BeEquivalentTo(1))
			})
		})
	})

	It("can create record with null value for optional field", func() {
		Context("having an object", func() {
			metaDescription :=  object.GetBaseMetaData(utils.RandomString(8))
			metaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString, Optional: true})
			metaObj, err := metaStore.NewMeta(metaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(metaObj)

			recordOne, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": nil}, auth.User{})
			Expect(err).To(BeNil())
			Expect(recordOne.Data["name"]).To(BeNil())
		})
	})

	It("Can create records containing reserved words", func() {
		Context("having an object named by reserved word and containing field named by reserved word", func() {
			metaDescription :=  object.GetBaseMetaData(utils.RandomString(8))
			metaDescription.AddField(&meta.Field{Name: "order", Type: meta.FieldTypeString})
			metaObj, err := metaStore.NewMeta(metaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(metaObj)
			Expect(err).To(BeNil())
			Context("and record has values containing reserved word", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"order": "order"}, auth.User{})
				Expect(err).To(BeNil())
				Expect(record.Data["id"]).To(Equal(float64(1)))

			})

		})

	})

	It("Can insert numeric value into string field", func() {
		Context("having an object with string field", func() {
			metaDescription := object.GetBaseMetaData(utils.RandomString(8))
			metaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString})
			metaObj, _ := metaStore.NewMeta(metaDescription)
			metaStore.Create(metaObj)

			Context("record can contain numeric value for string field", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": 202}, auth.User{})
				Expect(err).To(BeNil())
				Expect(record.Data["name"]).To(Equal("202"))
			})
		})
	})

	XIt("can create record with nested inner record at once", func() {
		aMeta := havingObjectA()
		bMeta := havingObjectB(aMeta)

		bData := map[string]interface{}{
			"a": map[string]interface{}{"name": "A record"},
		}
		user := auth.User{}
		record, err := dataProcessor.CreateRecord(bMeta.Name, bData, user)
		Expect(err).To(BeNil())
		Expect(record.Data).To(HaveKey("a"))
		aData := record.Data["a"].(map[string]interface{})
		Expect(aData).To(HaveKey("id"))
		Expect(aData["id"]).To(BeAssignableToTypeOf(1.0))
	})

	It("can create record with nested outer records at once", func() {
		aMeta := havingObjectA()
		bMeta := havingObjectB(aMeta)
		aMeta = havingObjectAWithManuallySetOuterLinkToB(aMeta, bMeta)

		aData := map[string]interface{}{
			"name":  "A record",
			"b_set": []interface{}{map[string]interface{}{"name": "B record"}},
		}
		user := auth.User{}
		record, err := dataProcessor.CreateRecord(aMeta.Name, aData, user)
		Expect(err).To(BeNil())
		Expect(record.Data).To(HaveKey("b_set"))
		bSetData := record.Data["b_set"].([]interface{})
		Expect(bSetData).To(HaveLen(1))
		bRecordData := bSetData[0].(map[string]interface{})
		Expect(bRecordData["id"]).To(BeAssignableToTypeOf(1.0))
	})

	It("can create record with nested outer records of mixed types(both new and existing) at once", func() {
		aMeta := havingObjectA()
		bMeta := havingObjectB(aMeta)
		aMeta = havingObjectAWithManuallySetOuterLinkToB(aMeta, bMeta)

		user := auth.User{}
		anotherARecord, err := dataProcessor.CreateRecord(aMeta.Name, map[string]interface{}{}, user)

		bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{"name": "Existing B record", "a": anotherARecord.Data["id"]}, user)
		Expect(err).To(BeNil())

		aData := map[string]interface{}{
			"name":  "A record",
			"b_set": []interface{}{map[string]interface{}{"name": "B record"}, bRecord.Pk()},
		}

		record, err := dataProcessor.CreateRecord(aMeta.Name, aData, user)
		Expect(err).To(BeNil())
		Expect(record.Data).To(HaveKey("b_set"))
		bSetData := record.Data["b_set"].([]interface{})
		Expect(bSetData).To(HaveLen(2))

		newBRecordData := bSetData[0].(map[string]interface{})
		Expect(newBRecordData["id"]).To(BeAssignableToTypeOf(1.0))
		Expect(newBRecordData["name"].(string)).To(Equal("B record"))

		existingBRecordData := bSetData[1].(map[string]interface{})
		Expect(existingBRecordData["id"]).To(BeAssignableToTypeOf(1.0))
		Expect(existingBRecordData["name"].(string)).To(Equal("Existing B record"))
	})

	It("can create record with nested records within 'Objects' field at once", func() {
		aMeta := havingObjectA()
		cMeta := havingObjectC()
		aMeta = havingObjectAWithObjectsLinkToB(aMeta, cMeta)

		aData := map[string]interface{}{
			"name": "A record",
			"cs":   []interface{}{map[string]interface{}{"name": "C record"}},
		}
		user := auth.User{}

		record, err := dataProcessor.CreateRecord(aMeta.Name, aData, user)

		//check returned data
		Expect(err).To(BeNil())
		Expect(record.Data).To(HaveKey("cs"))
		csData := record.Data["cs"].([]interface{})
		Expect(csData).To(HaveLen(1))
		cRecordData := csData[0].(map[string]interface{})
		Expect(cRecordData["id"]).To(BeAssignableToTypeOf(1.0))

		//ensure through object record has been created
		filter := fmt.Sprintf("eq(%s,%s)", aMeta.Name, record.PkAsString())
		_, matchedRecords, _ := dataProcessor.GetBulk(aMeta.FindField("cs").LinkThrough.Name, filter, nil, nil, 1, true)
		Expect(matchedRecords).To(HaveLen(1))
	})

	It("can create record with nested records within 'Objects' field at once with data of mixed type(both new and existing)", func() {
		aMeta := havingObjectA()
		cMeta := havingObjectC()
		aMeta = havingObjectAWithObjectsLinkToB(aMeta, cMeta)

		user := auth.User{}

		existingCRecord, err := dataProcessor.CreateRecord(
			cMeta.Name,
			map[string]interface{}{"name": "Existing C record"},
			user,
		)

		aData := map[string]interface{}{
			"name": "A record",
			"cs": []interface{}{
				map[string]interface{}{"name": "C record"},
				existingCRecord.Pk(),
			},
		}
		record, err := dataProcessor.CreateRecord(aMeta.Name, aData, user)
		Expect(err).To(BeNil())
		Expect(record.Data).To(HaveKey("cs"))
		csData := record.Data["cs"].([]interface{})
		Expect(csData).To(HaveLen(2))
		existingCRecordData := csData[1].(map[string]interface{})
		Expect(existingCRecordData[cMeta.Key]).To(Equal(existingCRecord.Pk()))
	})
})
