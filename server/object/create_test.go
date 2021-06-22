package object_test

import (
	"custodian/server/auth"
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Create test", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	dataManager, _ := object.NewDataManager(db)
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)
	dataProcessor, _ := object.NewProcessor(metaStore, dataManager, dbTransactionManager)
	testObjName := utils.RandomString(8)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	havingObjectA := func() *object.Meta {
		aMetaDescription := description.MetaDescription{
			Name: testObjName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&aMetaDescription)
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	havingObjectB := func() *object.Meta {
		bMetaDescription := description.MetaDescription{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     testObjName,
					Type:     description.FieldTypeObject,
					Optional: false,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjName,
					OnDelete: description.OnDeleteCascade.ToVerbose(),
				},
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&bMetaDescription)
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())
		return bMetaObj
	}

	havingObjectC := func() *object.Meta {
		cMetaDescription := description.MetaDescription{
			Name: "c",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&cMetaDescription)
		cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(cMetaObj)
		Expect(err).To(BeNil())
		return cMetaObj
	}

	havingObjectAWithManuallySetOuterLinkToB := func() *object.Meta {
		aMetaDescription := description.MetaDescription{
			Name: testObjName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
				{
					Name:           "b_set",
					Type:           description.FieldTypeArray,
					LinkType:       description.LinkTypeOuter,
					OuterLinkField: testObjName,
					LinkMeta:       "b",
					Optional:       true,
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&aMetaDescription)
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true, true)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	havingObjectAWithObjectsLinkToB := func() *object.Meta {
		aMetaDescription := description.MetaDescription{
			Name: testObjName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
				{
					Name:     "cs",
					Type:     description.FieldTypeObjects,
					LinkType: description.LinkTypeInner,
					LinkMeta: "c",
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&aMetaDescription)
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true, true)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	It("can create a record containing null value of foreign key field", func() {

		Context("having Reason object", func() {
			reasonMetaDescription := description.MetaDescription{
				Name: "test_reason",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeString,
						Optional: true,
					},
				},
			}
			reasonMetaObj, err := metaStore.NewMeta(&reasonMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(reasonMetaObj)
			Expect(err).To(BeNil())

			Context("and Lead object referencing Reason object", func() {
				leadMetaDescription := description.MetaDescription{
					Name: "test_lead",
					Key:  "id",
					Cas:  false,
					Fields: []description.Field{
						{
							Name: "id",
							Type: description.FieldTypeNumber,
							Def: map[string]interface{}{
								"func": "nextval",
							},
							Optional: true,
						},
						{
							Name: "name",
							Type: description.FieldTypeString,
						},
						{
							Name:     "decline_reason",
							Type:     description.FieldTypeObject,
							Optional: true,
							LinkType: description.LinkTypeInner,
							LinkMeta: "test_reason",
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
			metaDescription := description.MetaDescription{
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
						Name:     "name",
						Type:     description.FieldTypeString,
						Optional: true,
					},
				},
			}
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(metaObj)

			Context("DBManager creates record without any values", func() {
				record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{}, auth.User{})
				Expect(err).To(BeNil())
				Expect(record.Data["id"]).To(BeEquivalentTo(1))
			})
		})
	})

	It("can create record with null value for optional field", func() {
		Context("having an object", func() {
			metaDescription := description.MetaDescription{
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
						Name:     "name",
						Type:     description.FieldTypeString,
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
			metaDescription := description.MetaDescription{
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
						Name: "order",
						Type: description.FieldTypeString,
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
			metaDescription := description.MetaDescription{
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
						Name: "name",
						Type: description.FieldTypeString,
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
		havingObjectA()
		bMeta := havingObjectB()

		bData := map[string]interface{}{
			testObjName: map[string]interface{}{"name": "A record"},
		}
		user := auth.User{}
		record, err := dataProcessor.CreateRecord(bMeta.Name, bData, user)
		Expect(err).To(BeNil())
		Expect(record.Data).To(HaveKey(testObjName))
		aData := record.Data[testObjName].(map[string]interface{})
		Expect(aData).To(HaveKey("id"))
		Expect(aData["id"]).To(BeAssignableToTypeOf(1.0))
	})

	It("can create record with nested outer records at once", func() {
		aMeta := havingObjectA()
		havingObjectB()
		aMeta = havingObjectAWithManuallySetOuterLinkToB()

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
		bMeta := havingObjectB()
		aMeta = havingObjectAWithManuallySetOuterLinkToB()

		user := auth.User{}
		anotherARecord, err := dataProcessor.CreateRecord(aMeta.Name, map[string]interface{}{}, user)

		bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{"name": "Existing B record", testObjName: anotherARecord.Data["id"]}, user)
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
		havingObjectC()
		aMeta = havingObjectAWithObjectsLinkToB()

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
		aMeta = havingObjectAWithObjectsLinkToB()

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
		Expect(existingCRecordData[cMeta.Key.Name]).To(Equal(existingCRecord.Pk()))
	})

	It("can create a record to CamelCase field", func() {
		Context("having Reason object", func() {
			camelCaseDescription := description.MetaDescription{
				Name: "camel_case_description",
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
						Name:     "camelCaseString",
						Type:     description.FieldTypeString,
						Optional: true,
					},
				},
			}
			camelCaseObj, err := metaStore.NewMeta(&camelCaseDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(camelCaseObj)
			Expect(err).To(BeNil())

			Context("DBManager creates record without any values", func() {
				recordOne, err := dataProcessor.CreateRecord(camelCaseDescription.Name, map[string]interface{}{"camelCaseString": "CaMeL"}, auth.User{})
				Expect(err).To(BeNil())
				Expect(recordOne.Data["camelCaseString"]).To(Equal("CaMeL"))
			})
		})
	})

	It("can create a record to CamelCase enum field", func() {
		Context("having Reason object", func() {
			camelCaseDescription := description.MetaDescription{
				Name: "camel_case_description",
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
						Name:     "camelCaseEnum",
						Type:     description.FieldTypeEnum,
						Optional: true,
						Enum:     []string{"Camel"},
					},
				},
			}
			camelCaseObj, err := metaStore.NewMeta(&camelCaseDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(camelCaseObj)
			Expect(err).To(BeNil())

			Context("DBManager creates record without any values", func() {
				recordOne, err := dataProcessor.CreateRecord(camelCaseDescription.Name, map[string]interface{}{"camelCaseEnum": "Camel"}, auth.User{})
				Expect(err).To(BeNil())
				Expect(recordOne.Data["camelCaseEnum"]).To(Equal("Camel"))
			})
		})
	})
})
