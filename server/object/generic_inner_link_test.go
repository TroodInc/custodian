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
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache(), db)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)
	dataProcessor, _ := object.NewProcessor(metaStore, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can create a record containing generic inner value", func() {
		By("having two objects: A and B")
		testObjAName := utils.RandomString(8)
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
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("B contains generic inner field")
		testObjBName := utils.RandomString(8)
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name},
					Optional:     false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("having a record of object A")
		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		By("and having a record of object B containing generic field value with A object`s record")
		bRecord, err := dataProcessor.CreateRecord(
			bMetaObj.Name,
			map[string]interface{}{
				"target": map[string]interface{}{"_object": aMetaObj.Name, "id": aRecord.Data["id"]},
			},
			auth.User{},
		)
		Expect(err).To(BeNil())
		Expect(bRecord.Data["id"]).To(Equal(float64(1)))
		targetValue := bRecord.Data["target"].(map[string]interface{})
		Expect(targetValue["_object"]).To(Equal(aMetaObj.Name))
		Expect(targetValue["id"].(float64)).To(Equal(aRecord.Data["id"].(float64)))
	})

	It("cant create a record containing generic inner value with pk referencing not existing record", func() {
		By("having two objects: A and B")
		testObjAName := utils.RandomString(8)
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
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("B contains generic inner field")
		testObjBName := utils.RandomString(8)
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name},
					Optional:     false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("having a record of object B containing generic field value with A object`s record")
		_, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"target": map[string]interface{}{"_object": aMetaObj.Name, "id": 9999}}, auth.User{})
		Expect(err).To(Not(BeNil()))
		Expect(err.Error()).To(ContainSubstring("value does not exist"))
	})

	It("can update a record containing generic inner value", func() {
		By("having three objects: A, B and C")
		testObjAName := utils.RandomString(8)
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
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())
		testObjCName := utils.RandomString(8)
		cMetaDescription := description.MetaDescription{
			Name: testObjCName,
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
			},
		}
		cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(cMetaObj)
		Expect(err).To(BeNil())

		By("B contains generic inner field")
		testObjBName := utils.RandomString(8)
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name, cMetaObj.Name},
					Optional:     false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("having a record of object A")
		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		By("and having a record of object B containing generic field value with A object`s record")
		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"target": map[string]interface{}{"_object": aMetaObj.Name, "id": aRecord.Data["id"]}}, auth.User{})
		Expect(err).To(BeNil())

		By("this record is updated with record of object C")

		cRecord, err := dataProcessor.CreateRecord(cMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err = dataProcessor.UpdateRecord(bMetaObj.Name, strconv.Itoa(int(bRecord.Data["id"].(float64))), map[string]interface{}{"target": map[string]interface{}{"_object": cMetaObj.Name, "id": cRecord.Data["id"]}}, auth.User{})
		Expect(err).To(BeNil())
		Expect(bRecord.Data["id"]).To(Equal(float64(1)))
		targetValue := bRecord.Data["target"].(map[string]interface{})
		Expect(targetValue["_object"]).To(Equal(cMetaObj.Name))
		Expect(targetValue["id"]).To(Equal(bRecord.Data["id"].(float64)))
	})

	It("can update a record with null generic inner value", func() {
		By("having three objects: A, B and C")
		testObjAName := utils.RandomString(8)
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
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("B contains generic inner field")
		testObjBName := utils.RandomString(8)
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name},
					Optional:     true,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("having a record of object A")
		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		By("and having a record of object B containing generic field value with A object`s record")
		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"target": map[string]interface{}{"_object": aMetaObj.Name, "id": aRecord.Data["id"]}}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err = dataProcessor.UpdateRecord(bMetaObj.Name, strconv.Itoa(int(bRecord.Data["id"].(float64))), map[string]interface{}{"target": nil}, auth.User{})
		Expect(err).To(BeNil())
		Expect(bRecord.Data["id"]).To(Equal(float64(1)))
		Expect(bRecord.Data).To(HaveKey("target"))
		Expect(bRecord.Data["target"]).To(BeNil())
	})

	It("can update a record containing generic inner value without affecting value itself and it outputs generic value right", func() {
		By("having three objects: A, B and C")
		testObjAName := utils.RandomString(8)
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
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())
		testObjCName := utils.RandomString(8)
		cMetaDescription := description.MetaDescription{
			Name: testObjCName,
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
			},
		}
		cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(cMetaObj)
		Expect(err).To(BeNil())

		By("B contains generic inner field")
		testObjBName := utils.RandomString(8)
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name, cMetaObj.Name},
					Optional:     false,
				},
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("having a record of object A")
		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		By("and having a record of object B containing generic field value with A object`s record")
		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "record B", "target": map[string]interface{}{"_object": aMetaObj.Name, "id": aRecord.Data["id"]}}, auth.User{})
		Expect(err).To(BeNil())

		By("this record is updated with new value for regular field")

		bRecord, err = dataProcessor.UpdateRecord(bMetaObj.Name, strconv.Itoa(int(bRecord.Data["id"].(float64))), map[string]interface{}{"name": "Some other record B name"}, auth.User{})
		Expect(err).To(BeNil())
		data := bRecord.GetData()
		Expect(data["target"]).To(HaveKey(object.GenericInnerLinkObjectKey))
	})

	Describe("Retrieving records with generic values and casts PK value into its object PK type", func() {

		var aRecord *object.Record
		var bRecord *object.Record
		var err error

		testObjAName := utils.RandomString(8)
		testObjBName := utils.RandomString(8)
		testObjCName := utils.RandomString(8)
		testObjDName := utils.RandomString(8)

		havingObjectA := func() {
			By("having two objects: A and B")
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
						Optional: false,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())
		}

		havingObjectD := func() {
			By("having object D with ")
			dMetaDescription := description.MetaDescription{
				Name: testObjDName,
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
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjAName,
					},
				},
			}
			dMetaObj, err := metaStore.NewMeta(&dMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(dMetaObj)
			Expect(err).To(BeNil())
		}

		havingObjectAWithOuterLinkToD := func() {
			By("having object A with outer link to D")
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
						Optional: false,
					},
					{
						Name:           testObjDName + "_set",
						Type:           description.FieldTypeArray,
						LinkType:       description.LinkTypeOuter,
						LinkMeta:       testObjDName,
						OuterLinkField: testObjAName,
						RetrieveMode:   true,
						Optional:       true,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(aMetaObj.Name, aMetaObj, false, true)
			Expect(err).To(BeNil())
		}

		havingObjectBWithGenericLinkToA := func() {

			By("B contains generic inner field")
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
						Name:         "target",
						Type:         description.FieldTypeGeneric,
						LinkType:     description.LinkTypeInner,
						LinkMetaList: []string{testObjAName},
						Optional:     true,
					},
				},
			}
			bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())
		}

		havingObjectCWithGenericLinkToB := func() {

			By("C contains generic inner field")

			cMetaDescription := description.MetaDescription{
				Name: testObjCName,
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
						Name:         "target",
						Type:         description.FieldTypeGeneric,
						LinkType:     description.LinkTypeInner,
						LinkMetaList: []string{testObjBName},
						Optional:     true,
					},
				},
			}
			cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(cMetaObj)
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectA := func() {
			aRecord, err = dataProcessor.CreateRecord(testObjAName, map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectB := func() {
			bRecord, err = dataProcessor.CreateRecord(testObjBName, map[string]interface{}{"target": map[string]interface{}{"_object": testObjAName, "id": aRecord.Data["id"]}}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("can retrieve record containing generic inner value as a key", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having a record of object A", havingARecordOfObjectA)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectB)

			bRecord, err := dataProcessor.Get(testObjBName, strconv.Itoa(int(bRecord.Data["id"].(float64))), nil, nil, 1, false)
			Expect(err).To(BeNil())
			data := bRecord.GetData()
			targetValue := data["target"].(map[string]interface{})
			Expect(targetValue["_object"]).To(Equal(testObjAName))
			value, ok := targetValue["id"].(float64)
			Expect(ok).To(BeTrue())
			Expect(value).To(Equal(aRecord.Data["id"].(float64)))
		})

		It("can retrieve record containing generic inner value as a full object", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having a record of object A", havingARecordOfObjectA)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectB)

			bRecord, err := dataProcessor.Get(testObjBName, strconv.Itoa(int(bRecord.Data["id"].(float64))), nil, nil, 3, false)
			Expect(err).To(BeNil())
			targetValue := bRecord.Data["target"].(*object.Record)
			Expect(targetValue.Data["_object"]).To(Equal(testObjAName))
			Expect(targetValue.Data["id"].(float64)).To(Equal(aRecord.Data["id"].(float64)))
			Expect(targetValue.Data["name"].(string)).To(Equal(aRecord.Data["name"]))
		})

		It("can retrieve record containing nested generic relations", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B with generic link to A", havingObjectBWithGenericLinkToA)
			Describe("And having object C with generic link to B", havingObjectCWithGenericLinkToB)

			Describe("And having a record of object A", havingARecordOfObjectA)

			bRecord, err = dataProcessor.CreateRecord(testObjBName, map[string]interface{}{
				"target": map[string]interface{}{"_object": testObjAName, "id": aRecord.Data["id"]},
			}, auth.User{})
			Expect(err).To(BeNil())

			cRecord, err := dataProcessor.CreateRecord(testObjCName, map[string]interface{}{
				"target": map[string]interface{}{"_object": testObjBName, "id": bRecord.Data["id"]},
			}, auth.User{})
			Expect(err).To(BeNil())

			bRecord, err := dataProcessor.Get(testObjCName, strconv.Itoa(int(cRecord.Data["id"].(float64))), nil, nil, 3, false)
			Expect(err).To(BeNil())
			data := bRecord.GetData()
			Expect(data["target"].(map[string]interface{})["_object"].(string)).To(Equal(testObjBName))
			Expect(data["target"].(map[string]interface{})["target"].(map[string]interface{})["name"].(string)).To(Equal("A record"))
		})

		It("can retrieve record containing null generic inner value", func() {
			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having a record of object A", havingARecordOfObjectA)

			bRecord, err = dataProcessor.CreateRecord(testObjBName, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			bRecord, err := dataProcessor.Get(testObjBName, strconv.Itoa(int(bRecord.Data["id"].(float64))), nil, nil, 3, false)
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("target"))
			Expect(bRecord.Data["target"]).To(BeNil())
		})

		It("can retrieve record with generic value respecting specified depth", func() {
			havingObjectA()
			havingObjectD()
			havingObjectAWithOuterLinkToD()
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having a record of object A", havingARecordOfObjectA)

			_, err := dataProcessor.CreateRecord(
				testObjDName, map[string]interface{}{testObjAName: aRecord.Pk()}, auth.User{},
			)
			Expect(err).To(BeNil())

			bRecord, err = dataProcessor.CreateRecord(testObjBName, map[string]interface{}{
				"target": map[string]interface{}{"_object": testObjAName, "id": aRecord.Data["id"]},
			}, auth.User{})
			Expect(err).To(BeNil())

			bRecord, err = dataProcessor.Get(testObjBName, strconv.Itoa(int(bRecord.Data["id"].(float64))), nil, nil, 2, false)
			Expect(err).To(BeNil())

			_, ok := bRecord.Data["target"].(*object.Record).Data[testObjDName+"_set"].([]interface{})
			Expect(ok).To(BeTrue())
		})
	})

	Describe("Querying records by generic fields` values", func() {

		testObjAName := utils.RandomString(8)
		testObjBName := utils.RandomString(8)
		testObjCName := utils.RandomString(8)

		var aRecord *object.Record
		var bRecord *object.Record
		var cRecord *object.Record
		var err error

		havingObjectA := func() {
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
						Optional: false,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())
		}

		havingObjectC := func() {
			cMetaDescription := description.MetaDescription{
				Name: testObjCName,
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
				},
			}
			cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(cMetaObj)
			Expect(err).To(BeNil())
		}

		havingObjectBWithGenericLinkToAAndC := func() {

			By("B contains generic inner field")

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
						Name:         "target",
						Type:         description.FieldTypeGeneric,
						LinkType:     description.LinkTypeInner,
						LinkMetaList: []string{testObjAName, testObjCName},
						Optional:     true,
					},
				},
			}
			bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectA := func() {
			aRecord, err = dataProcessor.CreateRecord(testObjAName, map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectC := func() {
			cRecord, err = dataProcessor.CreateRecord(testObjCName, map[string]interface{}{"name": "C record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectA := func() {
			bRecord, err = dataProcessor.CreateRecord(testObjBName, map[string]interface{}{"target": map[string]interface{}{"_object": testObjAName, "id": aRecord.Data["id"]}}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectC := func() {
			bRecord, err = dataProcessor.CreateRecord(testObjBName, map[string]interface{}{"target": map[string]interface{}{"_object": testObjCName, "id": cRecord.Data["id"]}}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("can retrieve record with generic field as key by querying by A record`s field", func() {

			Describe("Having object A", havingObjectA)
			Describe("Having object C", havingObjectC)
			Describe("And having object B", havingObjectBWithGenericLinkToAAndC)
			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("And having a record of object C", havingARecordOfObjectC)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)
			Describe("and having a record of object B containing null generic field value ", havingARecordOfObjectBContainingRecordOfObjectC)

			_, matchedRecords, err := dataProcessor.GetBulk(
				testObjBName,
				fmt.Sprintf("eq(target.%s.name,A record)", testObjAName),
				nil, nil, 1, false,
			)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
			targetValue := matchedRecords[0].Data["target"].(*object.GenericInnerLink).AsMap()
			Expect(targetValue["_object"]).To(Equal(testObjAName))
			Expect(targetValue["id"].(float64)).To(Equal(aRecord.Data["id"].(float64)))

		})

		It("can retrieve record with generic field as full object by querying by A record`s field", func() {

			Describe("Having object A", havingObjectA)
			Describe("Having object C", havingObjectC)
			Describe("And having object B", havingObjectBWithGenericLinkToAAndC)
			Describe("And having a record of object A", havingARecordOfObjectA)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			_, matchedRecords, err := dataProcessor.GetBulk(
				testObjBName,
				fmt.Sprintf("eq(target.%s.name,A record)", testObjAName),
				nil, nil, 2, false,
			)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
			targetValue := matchedRecords[0].Data["target"].(*object.Record)
			Expect(targetValue.Data["_object"].(string)).To(Equal(testObjAName))
			Expect(targetValue.Data["id"].(float64)).To(Equal(aRecord.Data["id"].(float64)))
			Expect(targetValue.Data["name"].(string)).To(Equal(aRecord.Data["name"].(string)))
		})

		It("can query records by generic_field's type", func() {

			Describe("Having object A", havingObjectA)
			Describe("Having object C", havingObjectC)
			Describe("And having object B", havingObjectBWithGenericLinkToAAndC)
			Describe("And having a record of object A", havingARecordOfObjectA)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			_, err = dataProcessor.CreateRecord(testObjBName, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			_, matchedRecords, err := dataProcessor.GetBulk(
				testObjBName,
				fmt.Sprintf("eq(target._object,%s)", testObjAName),
				nil, nil, 2, false,
			)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
			targetValue := matchedRecords[0].Data["target"].(*object.Record)
			Expect(targetValue.Data["_object"].(string)).To(Equal(testObjAName))
			Expect(targetValue.Data["id"].(float64)).To(Equal(aRecord.Data["id"].(float64)))
			Expect(targetValue.Data["name"].(string)).To(Equal(aRecord.Data["name"].(string)))
		})

		It("can create record with nested new inner generic record", func() {

			Describe("Having object A", havingObjectA)
			Describe("Having object C", havingObjectC)
			Describe("And having object B", havingObjectBWithGenericLinkToAAndC)

			bRecordData := map[string]interface{}{"target": map[string]interface{}{"_object": testObjAName, "name": "Some A record"}}

			bRecord, err = dataProcessor.CreateRecord(testObjBName, bRecordData, auth.User{})
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("target"))
			Expect(bRecord.Data["target"]).To(Not(BeNil()))
		})

		It("can create record with nested existing inner generic record", func() {

			Describe("Having object A", havingObjectA)
			Describe("Having object C", havingObjectC)
			Describe("And having object B", havingObjectBWithGenericLinkToAAndC)

			existingARecord, err := dataProcessor.CreateRecord(
				testObjAName,
				map[string]interface{}{"name": "Existing A record"},
				auth.User{},
			)
			Expect(err).To(BeNil())

			bRecordData := map[string]interface{}{"target": map[string]interface{}{"_object": testObjAName, "id": existingARecord.Data["id"]}}

			bRecord, err = dataProcessor.CreateRecord(testObjBName, bRecordData, auth.User{})
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("target"))
			Expect(bRecord.Data["target"]).To(Not(BeNil()))
			Expect(bRecord.Data["target"].(map[string]interface{})["id"]).To(Equal(existingARecord.Data["id"]))
		})
	})
})
