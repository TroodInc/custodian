package data_test

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/auth"
	"server/data"
	"server/object"

	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("Create test", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := object.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	havingObjectA := func() *object.Meta {
		aMetaDescription := object.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "name",
					Type:     object.FieldTypeString,
					Optional: true,
				},
			},
		}
		(&object.NormalizationService{}).Normalize(&aMetaDescription)
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	havingObjectB := func(A *object.Meta) *object.Meta {
		bMetaDescription := object.Meta{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "a",
					Type:     object.FieldTypeObject,
					Optional: false,
					LinkType: object.LinkTypeInner,
					LinkMeta: A,
					OnDelete: object.OnDeleteCascade.ToVerbose(),
				},
				{
					Name:     "name",
					Type:     object.FieldTypeString,
					Optional: true,
				},
			},
		}
		(&object.NormalizationService{}).Normalize(&bMetaDescription)
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())
		return bMetaObj
	}

	havingObjectC := func() *object.Meta {
		cMetaDescription := object.Meta{
			Name: "c",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "name",
					Type:     object.FieldTypeString,
					Optional: true,
				},
			},
		}
		(&object.NormalizationService{}).Normalize(&cMetaDescription)
		cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(cMetaObj)
		Expect(err).To(BeNil())
		return cMetaObj
	}

	havingObjectAWithManuallySetOuterLinkToB := func(B *object.Meta) *object.Meta {
		aMetaDescription := object.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "name",
					Type:     object.FieldTypeString,
					Optional: true,
				},
				{
					Name:           "b_set",
					Type:           object.FieldTypeArray,
					LinkType:       object.LinkTypeOuter,
					OuterLinkField: B.FindField("a"),
					LinkMeta:       B,
					Optional:       true,
				},
			},
		}
		(&object.NormalizationService{}).Normalize(&aMetaDescription)
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	havingObjectAWithObjectsLinkToB := func(C *object.Meta) *object.Meta {
		aMetaDescription := object.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "name",
					Type:     object.FieldTypeString,
					Optional: true,
				},
				{
					Name:     "cs",
					Type:     object.FieldTypeObjects,
					LinkType: object.LinkTypeInner,
					LinkMeta: C,
				},
			},
		}
		(&object.NormalizationService{}).Normalize(&aMetaDescription)
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	XIt("can create a record containing null value of foreign key field", func() {

		Context("having Reason object", func() {
			reasonMetaDescription := object.Meta{
				Name: "test_reason",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
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
				leadMetaDescription := object.Meta{
					Name: "test_lead",
					Key:  "id",
					Cas:  false,
					Fields: []*object.Field{
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
							LinkMeta: reasonMetaObj,
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
					record, err := dataProcessor.CreateRecord(leadMetaDescription.Name, leadData, user)
					Expect(err).To(BeNil())
					Expect(record.Data).To(HaveKey("decline_reason"))
				})
			})
		})
	})

	It("can create record without specifying any value", func() {
		Context("having an object with optional fields", func() {
			metaDescription := object.Meta{
				Name: "test_order",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
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
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{}, auth.User{})
				Expect(err).To(BeNil())
				Expect(record.Data["id"]).To(BeEquivalentTo(1))
			})
		})
	})

	It("can create record with null value for optional field", func() {
		Context("having an object", func() {
			metaDescription := object.Meta{
				Name: "test_order",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
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
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(metaObj)

			recordOne, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": nil}, auth.User{})
			Expect(err).To(BeNil())
			Expect(recordOne.Data["name"]).To(BeNil())
		})
	})

	It("Can create records containing reserved words", func() {
		Context("having an object named by reserved word and containing field named by reserved word", func() {
			metaDescription := object.Meta{
				Name: "test_order",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
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
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"order": "order"}, auth.User{})
				Expect(err).To(BeNil())
				Expect(record.Data["id"]).To(Equal(float64(1)))

			})

		})

	})

	It("Can insert numeric value into string field", func() {
		Context("having an object with string field", func() {
			metaDescription := object.Meta{
				Name: "test_order",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
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
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": 202}, auth.User{})
				Expect(err).To(BeNil())
				Expect(record.Data["name"]).To(Equal("202"))
			})
		})
	})

	It("can create record with nested inner record at once", func() {
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
		aMeta = havingObjectAWithManuallySetOuterLinkToB(bMeta)

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
		aMeta = havingObjectAWithManuallySetOuterLinkToB(bMeta)

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
		aMeta = havingObjectAWithObjectsLinkToB(cMeta)

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
		aMeta = havingObjectAWithObjectsLinkToB(cMeta)

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
