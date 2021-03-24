package object_test

import (
	"custodian/server/auth"
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"
	"fmt"
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := object.NewStore(metaDescriptionSyncer, syncer, dbTransactionManager)
	dataProcessor, _ := object.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})
	testObjAName := utils.RandomString(8)
	testObjBName := utils.RandomString(8)
	testObjCName := utils.RandomString(8)
	testObjDName := utils.RandomString(8)
	testObjBSetName := fmt.Sprintf("%s_set", testObjBName)
	testObjCSetName := fmt.Sprintf("%s_set", testObjCName)
	testObjWithEnumName := fmt.Sprintf("%s_enum", utils.RandomString(8))

	It("Can update records containing reserved words", func() {
		Context("having an object named by reserved word and containing field named by reserved word", func() {

			metaDescription := description.MetaDescription{
				Name: "order",
				Key:  "order",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "order",
						Type:     description.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name: "select",
						Type: description.FieldTypeString,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			Context("and record of this object", func() {
				record, err := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"select": "some value"}, auth.User{})
				Expect(err).To(BeNil())
				Context("is being updated with values containing reserved word", func() {
					record, err := dataProcessor.UpdateRecord(metaObj.Name, strconv.Itoa(int(record.Data["order"].(float64))), map[string]interface{}{"select": "select"}, auth.User{})
					Expect(err).To(BeNil())
					Expect(record.Data["select"]).To(Equal("select"))
				})

			})

		})

	})

	It("Can perform bulk update", func() {
		By("Having Position object")

		positionMetaDescription := description.MetaDescription{
			Name: "position",
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
		metaObj, err := metaStore.NewMeta(&positionMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and Person object")

		metaDescription := description.MetaDescription{
			Name: "person",
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
					Name:     "position",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "position",
				},
				{
					Name: "name",
					Type: description.FieldTypeString,
				},
			},
		}
		metaObj, err = metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and having one record of Position object")
		positionRecord, err := dataProcessor.CreateRecord(positionMetaDescription.Name, map[string]interface{}{"name": "manager"}, auth.User{})
		Expect(err).To(BeNil())

		By("and having two records of Person object")

		records := make([]*object.Record, 2)

		records[0], err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "Ivan", "position": positionRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		records[1], err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "Vasily", "position": positionRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		updatedRecords := make([]map[string]interface{}, 0)

		Context("person records are updated with new name value and new position`s name value as nested object", func() {
			counter := 0
			next := func() (map[string]interface{}, error) {
				if counter < len(records) {
					records[counter].Data["name"] = "Victor"
					records[counter].Data["position"] = map[string]interface{}{"id": positionRecord.Data["id"], "name": "sales manager"}
					defer func() { counter += 1 }()
					return records[counter].Data, nil
				}
				return nil, nil
			}

			sink := func(record map[string]interface{}) error {
				updatedRecords = append(updatedRecords, record)
				return nil
			}
			err := dataProcessor.BulkUpdateRecords(metaDescription.Name, next, sink, auth.User{})
			Expect(err).To(BeNil())

			Expect(updatedRecords[0]["name"]).To(Equal("Victor"))

			positionRecordData, _ := updatedRecords[0]["position"].(map[string]interface{})
			Expect(positionRecordData["name"]).To(Equal("sales manager"))

		})

	})

	It("Can perform update", func() {
		By("Having Position object")

		positionMetaDescription := description.MetaDescription{
			Name: "position",
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
		metaObj, err := metaStore.NewMeta(&positionMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and having one record of Position object")
		record, err := dataProcessor.CreateRecord(positionMetaDescription.Name, map[string]interface{}{"name": "manager"}, auth.User{})
		Expect(err).To(BeNil())

		keyValue, _ := record.Data["id"].(float64)
		Context("person records are updated with new name value and new position`s name value as nested object", func() {
			record.Data["name"] = "sales manager"
			record, err = dataProcessor.UpdateRecord(positionMetaDescription.Name, strconv.Itoa(int(keyValue)), record.Data, auth.User{})
			Expect(err).To(BeNil())

			Expect(record.Data["name"]).To(Equal("sales manager"))

		})

	})

	It("Can update record with null value", func() {
		By("Having A object")

		positionMetaDescription := description.MetaDescription{
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
		metaObj, err := metaStore.NewMeta(&positionMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and having one record of A object")
		record, err := dataProcessor.CreateRecord(positionMetaDescription.Name, map[string]interface{}{"name": ""}, auth.User{})
		Expect(err).To(BeNil())

		keyValue, _ := record.Data["id"].(float64)
		Context("person records are updated with new name value and new position`s name value as nested object", func() {
			record.Data["name"] = nil
			record, err := dataProcessor.UpdateRecord(positionMetaDescription.Name, strconv.Itoa(int(keyValue)), record.Data, auth.User{})
			Expect(err).To(BeNil())

			Expect(record.Data["name"]).To(BeNil())
		})

	})

	havingObjectA := func() *object.Meta {
		aMetaDescription := description.MetaDescription{
			Name: testObjAName,
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
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		(&description.NormalizationService{}).Normalize(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	havingObjectAWithObjectsLinkToD := func() *object.Meta {
		aMetaDescription := description.MetaDescription{
			Name: testObjAName,
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
					Name:     "ds",
					Type:     description.FieldTypeObjects,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjDName,
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

	havingObjectB := func(onDelete string) *object.Meta {
		bMetaDescription := description.MetaDescription{
			Name: testObjBName,
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
					Name:     testObjAName,
					Type:     description.FieldTypeObject,
					Optional: true,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjAName,
					OnDelete: onDelete,
				},
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&bMetaDescription)
		(&description.NormalizationService{}).Normalize(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	havingObjectC := func() *object.Meta {
		metaDescription := description.MetaDescription{
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
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
				{
					Name:     testObjBName,
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjBName,
					Optional: true,
				},
				{
					Name:     testObjDName,
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjDName,
					Optional: true,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	havingObjectD := func() *object.Meta {
		metaDescription := description.MetaDescription{
			Name: testObjDName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeString,
					Optional: false,
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
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	havingObjectEnum := func() *object.Meta {
		metaDescription := description.MetaDescription{
			Name: testObjWithEnumName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeString,
					Optional: false,
				},
				{
					Name:     "enumField",
					Type:     description.FieldTypeEnum,
					Optional: true,
					Enum:     description.EnumChoices{"1", "2"},
				},
			},
		}
		//create enum statement

		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	factoryObjectBWithManuallySetOuterLinkToC := func() *object.Meta {
		metaDescription := description.MetaDescription{
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
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
				{
					Name:     testObjAName,
					Type:     description.FieldTypeObject,
					Optional: true,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjAName,
					OnDelete: description.OnDeleteCascade.ToVerbose(),
				},
				{
					Name:           testObjCSetName,
					Type:           description.FieldTypeArray,
					LinkType:       description.LinkTypeOuter,
					LinkMeta:       testObjCName,
					OuterLinkField: testObjBName,
					Optional:       true,
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&metaDescription)
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(metaObj.Name, metaObj, true, true)
		Expect(err).To(BeNil())
		return metaObj
	}

	havingObjectAWithManuallySetOuterLink := func() *object.Meta {

		aMetaDescription := description.MetaDescription{
			Name: testObjAName,
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
					Name:     testObjBSetName,
					Type:     description.FieldTypeArray,
					LinkType: description.LinkTypeOuter,
					LinkMeta: testObjBName, OuterLinkField: testObjAName,
					Optional: true,
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		(&description.NormalizationService{}).Normalize(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true, true)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	It("Can create record with enum values", func() {
		enumObj := havingObjectEnum()

		// can create enum record
		_, err := dataProcessor.CreateRecord(enumObj.Name, map[string]interface{}{"id": "enumRecord", "enumField": "1"}, auth.User{})
		Expect(err).To(BeNil())

		// can't create records with values not in enum choices
		_, err = dataProcessor.CreateRecord(enumObj.Name, map[string]interface{}{"id": "invalid enumRecord", "enumField": "4"}, auth.User{})
		Expect(err).NotTo(BeNil())
	})

	It("Can perform update of record with nested inner record at once", func() {
		aMetaObj := havingObjectA()
		bMetaObj := havingObjectB(description.OnDeleteCascade.ToVerbose())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "B record", testObjAName: aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		bUpdatedData := map[string]interface{}{
			"id":          bRecord.Pk(),
			"name":        "Updated B name",
			aMetaObj.Name: map[string]interface{}{"id": aRecord.Data["id"], "name": "Updated A name"},
		}
		record, err := dataProcessor.UpdateRecord(bMetaObj.Name, bRecord.PkAsString(), bUpdatedData, auth.User{})
		Expect(err).To(BeNil())
		Expect(record.Data).To(HaveKey(testObjAName))
	})

	It("Can perform update of record with nested inner record at once", func() {
		aMetaObj := havingObjectA()
		bMetaObj := havingObjectB(description.OnDeleteCascade.ToVerbose())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "B record", testObjAName: aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		bUpdatedData := map[string]interface{}{
			"id":          bRecord.Pk(),
			"name":        "Updated B name",
			aMetaObj.Name: map[string]interface{}{"id": aRecord.Data["id"], "name": "Updated A name"},
		}
		recordData, err := dataProcessor.UpdateRecord(bMetaObj.Name, bRecord.PkAsString(), bUpdatedData, auth.User{})
		Expect(err).To(BeNil())
		Expect(recordData.Data).To(HaveKey(testObjAName))
	})

	It("Can perform update of record with nested outer records of mixed types: new record, existing record`s PK and existing record`s new data at once", func() {
		havingObjectA()
		bMetaObj := havingObjectB(description.OnDeleteCascade.ToVerbose())
		aMetaObj := havingObjectAWithManuallySetOuterLink()

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "B record", testObjAName: aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		anotherBRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "Another B record", testObjAName: aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aUpdateData := map[string]interface{}{
			"id":   aRecord.PkAsString(),
			"name": "Updated A name",
			testObjBSetName: []interface{}{
				map[string]interface{}{"id": bRecord.Data["id"], "name": "Updated B name"}, //existing record with new data
				anotherBRecord.Data["id"],                      //existing record`s PK
				map[string]interface{}{"name": "New B Record"}, //new record`s data
			},
		}

		record, err := dataProcessor.UpdateRecord(aMetaObj.Name, aRecord.PkAsString(), aUpdateData, auth.User{})
		Expect(err).To(BeNil())
		Expect(record.Data).To(HaveKey(testObjBSetName))
		bSetData := record.Data[testObjBSetName].([]interface{})
		Expect(bSetData).To(HaveLen(3))
		Expect(bSetData[0].(map[string]interface{})["name"]).To(Equal("Updated B name"))
		Expect(bSetData[1].(map[string]interface{})["name"]).To(Equal("Another B record"))
		Expect(bSetData[2].(map[string]interface{})["name"]).To(Equal("New B Record"))
	})

	It("Processes delete logic for outer records which are not presented in update data. `Cascade` strategy case ", func() {
		havingObjectA()
		bMetaObj := havingObjectB(description.OnDeleteCascade.ToVerbose())
		aMetaObj := havingObjectAWithManuallySetOuterLink()

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "B record", testObjAName: aRecord.Pk()}, auth.User{})
		Expect(err).To(BeNil())

		anotherBRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "Another B record", testObjAName: aRecord.Pk()}, auth.User{})
		Expect(err).To(BeNil())

		//anotherBRecord`s id is not set
		aUpdateData := map[string]interface{}{
			"id":   aRecord.PkAsString(),
			"name": "Updated A name",
			testObjBSetName: []interface{}{
				bRecord.Pk(), //existing record with new data
				map[string]interface{}{"name": "New B Record"}, //new record`s data
			},
		}

		obj, err := dataProcessor.UpdateRecord(aMetaObj.Name, aRecord.PkAsString(), aUpdateData, auth.User{})
		Expect(err).To(BeNil())

		//check returned data
		Expect(obj.Data).To(HaveKey(testObjBSetName))
		bSetData := obj.Data[testObjBSetName].([]interface{})
		Expect(bSetData).To(HaveLen(2))
		Expect(bSetData[0].(map[string]interface{})["name"]).To(Equal("B record"))
		Expect(bSetData[1].(map[string]interface{})["name"]).To(Equal("New B Record"))
		//	check queried data
		obj, err = dataProcessor.Get(aMetaObj.Name, aRecord.PkAsString(), nil, nil, 2, false)
		Expect(err).To(BeNil())
		Expect(obj.Data).To(HaveKey(testObjBSetName))
		bSetData = obj.Data[testObjBSetName].([]interface{})
		Expect(bSetData).To(HaveLen(2))
		Expect(bSetData[0].(*object.Record).Data["name"]).To(Equal("B record"))
		Expect(bSetData[1].(*object.Record).Data["name"]).To(Equal("New B Record"))
		//	check B record is deleted
		removedBRecordPk, _ := bMetaObj.Key.ValueAsString(anotherBRecord.Data["id"])
		obj, err = dataProcessor.Get(bMetaObj.Name, removedBRecordPk, nil, nil, 1, false)
		Expect(err).To(BeNil())
		Expect(obj).To(BeNil())
	})

	It("Processes delete logic for outer records which are not presented in update data. `SetNull` strategy case ", func() {
		havingObjectA()
		bMetaObj := havingObjectB(description.OnDeleteSetNull.ToVerbose())
		aMetaObj := havingObjectAWithManuallySetOuterLink()

		aRecord, err := dataProcessor.CreateRecord(
			aMetaObj.Name, map[string]interface{}{"name": "A record"}, auth.User{},
		)
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(
			bMetaObj.Name, map[string]interface{}{"name": "B record", testObjAName: aRecord.Data["id"]}, auth.User{},
		)
		Expect(err).To(BeNil())

		anotherBRecord, err := dataProcessor.CreateRecord(
			bMetaObj.Name, map[string]interface{}{"name": "Another B record", testObjAName: aRecord.Data["id"]}, auth.User{},
		)
		Expect(err).To(BeNil())

		//anotherBRecord`s id is not set
		aUpdateData := map[string]interface{}{
			"id":   aRecord.Pk(),
			"name": "Updated A name",
			testObjBSetName: []interface{}{
				bRecord.Data["id"], //existing record with new data
				map[string]interface{}{"name": "New B Record"}, //new record`s data
			},
		}

		obj, err := dataProcessor.UpdateRecord(aMetaObj.Name, aRecord.PkAsString(), aUpdateData, auth.User{})
		Expect(err).To(BeNil())

		//check returned data
		Expect(obj.Data).To(HaveKey(testObjBSetName))
		bSetData := obj.Data[testObjBSetName].([]interface{})
		Expect(bSetData).To(HaveLen(2))
		Expect(bSetData[0].(map[string]interface{})["name"]).To(Equal("B record"))
		Expect(bSetData[1].(map[string]interface{})["name"]).To(Equal("New B Record"))
		//	check queried data
		obj, err = dataProcessor.Get(aMetaObj.Name, aRecord.PkAsString(), nil, nil, 2, false)
		Expect(err).To(BeNil())
		Expect(obj.Data).To(HaveKey(testObjBSetName))
		bSetData = obj.Data[testObjBSetName].([]interface{})
		Expect(bSetData).To(HaveLen(2))
		Expect(bSetData[0].(*object.Record).Data["name"]).To(Equal("B record"))
		Expect(bSetData[1].(*object.Record).Data["name"]).To(Equal("New B Record"))
		//	check B record is not deleted
		removedBRecordPk, _ := bMetaObj.Key.ValueAsString(anotherBRecord.Data["id"])
		obj, err = dataProcessor.Get(bMetaObj.Name, removedBRecordPk, nil, nil, 1, false)
		Expect(err).To(BeNil())
		Expect(obj).NotTo(BeNil())
	})

	It("Processes delete logic for outer records which are not presented in update data. `Restrict` strategy case ", func() {
		havingObjectA()
		bMetaObj := havingObjectB(description.OnDeleteRestrict.ToVerbose())
		aMetaObj := havingObjectAWithManuallySetOuterLink()

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "B record", testObjAName: aRecord.Pk()}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "Another B record", testObjAName: aRecord.Pk()}, auth.User{})
		Expect(err).To(BeNil())

		//anotherBRecord`s id is not set
		aUpdateData := map[string]interface{}{
			"id":   aRecord.Pk(),
			"name": "Updated A name",
			testObjBSetName: []interface{}{
				bRecord.Pk(), //existing record with new data
				map[string]interface{}{"name": "New B Record"}, //new record`s data
			},
		}

		_, err = dataProcessor.UpdateRecord(aMetaObj.Name, aRecord.PkAsString(), aUpdateData, auth.User{})
		Expect(err).NotTo(BeNil())
	})

	It("Updates record with nested records with mixed values(both valuable and null)", func() {
		aMetaObj := havingObjectA()
		bMetaObj := havingObjectB(description.OnDeleteRestrict.ToVerbose())
		dMetaObj := havingObjectD()
		cMetaObj := havingObjectC()

		factoryObjectBWithManuallySetOuterLinkToC()

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "B record", testObjAName: aRecord.Pk()}, auth.User{})
		Expect(err).To(BeNil())

		cRecordWithNilA, err := dataProcessor.CreateRecord(cMetaObj.Name, map[string]interface{}{"name": "C record", testObjAName: nil, testObjBName: bRecord.Pk()}, auth.User{})
		Expect(err).To(BeNil())

		cRecordWithValuableA, err := dataProcessor.CreateRecord(cMetaObj.Name, map[string]interface{}{"name": "C record", testObjAName: aRecord.Pk(), testObjBName: bRecord.Pk()}, auth.User{})
		Expect(err).To(BeNil())

		dRecord, err := dataProcessor.CreateRecord(dMetaObj.Name, map[string]interface{}{"id": "DRecord", "name": "DDDrecord"}, auth.User{})
		Expect(err).To(BeNil())

		//anotherBRecord`s id is not set
		bUpdateData := map[string]interface{}{
			"id":   bRecord.Pk(),
			"name": "Updated B name",
			testObjCSetName: []interface{}{
				map[string]interface{}{"id": cRecordWithNilA.Pk(), testObjDName: nil},
				map[string]interface{}{"id": cRecordWithValuableA.Pk(), testObjDName: dRecord.Pk()}, //new record`s data
			},
		}

		bRecord, err = dataProcessor.UpdateRecord(bMetaObj.Name, bRecord.PkAsString(), bUpdateData, auth.User{})
		Expect(err).To(BeNil())
		Expect(bRecord.Data).To(HaveKeyWithValue("name", "Updated B name"))
		Expect(bRecord.Data[testObjCSetName].([]interface{})[0].(map[string]interface{})[testObjDName]).To(BeNil())
		Expect(bRecord.Data[testObjCSetName].([]interface{})[1].(map[string]interface{})).To(HaveKeyWithValue(testObjDName, "DRecord"))
	})

	It("Processes delete logic for records within 'Objects' relation which are not presented in update data. Case 1: uniform type of data(list of ids)", func() {
		havingObjectA()
		dMetaObj := havingObjectD()
		aMetaObj := havingObjectAWithObjectsLinkToD()

		dRecord, err := dataProcessor.CreateRecord(dMetaObj.Name, map[string]interface{}{"name": "D record", "id": "rec"}, auth.User{})
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A record", "ds": []interface{}{dRecord.Pk()}}, auth.User{})
		Expect(err).To(BeNil())

		anotherDRecord, err := dataProcessor.CreateRecord(dMetaObj.Name, map[string]interface{}{"name": "Another D record", "id": "another-rec"}, auth.User{})
		Expect(err).To(BeNil())

		//anotherBRecord`s id is not set
		aUpdateData := map[string]interface{}{
			"id":   aRecord.Pk(),
			"name": "Updated A name",
			"ds":   []interface{}{anotherDRecord.Pk()},
		}

		updatedARecord, err := dataProcessor.UpdateRecord(aMetaObj.Name, aRecord.PkAsString(), aUpdateData, auth.User{})
		Expect(err).To(BeNil())
		Expect(updatedARecord.Data).To(HaveKey("ds"))
		Expect(updatedARecord.Data["ds"]).To(HaveLen(1))
		Expect(updatedARecord.Data["ds"].([]interface{})[0].(map[string]interface{})["id"]).To(Equal(anotherDRecord.Pk()))
	})

	It("Processes delete logic for records within 'Objects' relation which are not presented in update data. Case 2: mixed type of data", func() {
		havingObjectA()
		dMetaObj := havingObjectD()
		aMetaObj := havingObjectAWithObjectsLinkToD()

		dRecord, err := dataProcessor.CreateRecord(dMetaObj.Name, map[string]interface{}{"name": "D record", "id": "rec"}, auth.User{})
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A record", "ds": []interface{}{dRecord.Pk()}}, auth.User{})
		Expect(err).To(BeNil())

		anotherDRecord, err := dataProcessor.CreateRecord(dMetaObj.Name, map[string]interface{}{"name": "Another D record", "id": "another-rec"}, auth.User{})
		Expect(err).To(BeNil())

		//anotherBRecord`s id is not set
		aUpdateData := map[string]interface{}{
			"id":   aRecord.Pk(),
			"name": "Updated A name",
			"ds":   []interface{}{anotherDRecord.Pk(), map[string]interface{}{"name": "D record", "id": "rec"}},
		}

		updatedARecord, err := dataProcessor.UpdateRecord(aMetaObj.Name, aRecord.PkAsString(), aUpdateData, auth.User{})
		Expect(err).To(BeNil())
		Expect(updatedARecord.Data).To(HaveKey("ds"))
		Expect(updatedARecord.Data["ds"]).To(HaveLen(2))
		Expect(updatedARecord.Data["ds"].([]interface{})[0].(map[string]interface{})["id"]).To(Equal(anotherDRecord.Pk()))
	})

	It("Processes delete logic for records within 'Objects' relation if empty list is specified", func() {
		havingObjectA()
		dMetaObj := havingObjectD()
		aMetaObj := havingObjectAWithObjectsLinkToD()

		dRecord, err := dataProcessor.CreateRecord(dMetaObj.Name, map[string]interface{}{"name": "D record", "id": "rec"}, auth.User{})
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A record", "ds": []interface{}{dRecord.Pk()}}, auth.User{})
		Expect(err).To(BeNil())

		//anotherBRecord`s id is not set
		aUpdateData := map[string]interface{}{
			"id":   aRecord.Pk(),
			"name": "Updated A name",
			"ds":   []interface{}{},
		}

		_, err = dataProcessor.UpdateRecord(aMetaObj.Name, aRecord.PkAsString(), aUpdateData, auth.User{})
		Expect(err).To(BeNil())

		aRecord, err = dataProcessor.Get(aMetaObj.Name, aRecord.PkAsString(), nil, nil, 1, false)
		Expect(err).To(BeNil())

		Expect(aRecord.Data).To(HaveKey("ds"))
		Expect(aRecord.Data["ds"]).To(HaveLen(0))
	})

})
