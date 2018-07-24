package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
	"server/pg"
	"server/data"
	"utils"
	"server/auth"
	"strconv"
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

	It("can create a record containing generic inner value", func() {
		By("having two objects: A and B")
		aMetaDescription := meta.MetaDescription{
			Name: "a",
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
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("B contains generic inner field")

		bMetaDescription := meta.MetaDescription{
			Name: "b",
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
					Name:         "target",
					Type:         meta.FieldTypeGeneric,
					LinkType:     meta.LinkTypeInner,
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
		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{}, true)
		Expect(err).To(BeNil())

		By("and having a record of object B containing generic field value with A object`s record")
		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"target": map[string]interface{}{"_object": aMetaObj.Name, "id": aRecord["id"]}}, auth.User{}, true)
		Expect(err).To(BeNil())
		Expect(bRecord["id"]).To(Equal(float64(1)))
		targetValue := bRecord["target"].(map[string]string)
		Expect(targetValue["_object"]).To(Equal(aMetaObj.Name))
		Expect(strconv.Atoi(targetValue["id"])).To(Equal(int(aRecord["id"].(float64))))
	})

	It("can update a record containing generic inner value", func() {
		By("having three objects: A, B and C")
		aMetaDescription := meta.MetaDescription{
			Name: "a",
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
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		cMetaDescription := meta.MetaDescription{
			Name: "c",
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
			},
		}
		cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(cMetaObj)
		Expect(err).To(BeNil())

		By("B contains generic inner field")

		bMetaDescription := meta.MetaDescription{
			Name: "b",
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
					Name:         "target",
					Type:         meta.FieldTypeGeneric,
					LinkType:     meta.LinkTypeInner,
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
		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{}, true)
		Expect(err).To(BeNil())

		By("and having a record of object B containing generic field value with A object`s record")
		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"target": map[string]interface{}{"_object": aMetaObj.Name, "id": aRecord["id"]}}, auth.User{}, true)
		Expect(err).To(BeNil())

		By("this record is updated with record of object C")

		cRecord, err := dataProcessor.CreateRecord(cMetaObj.Name, map[string]interface{}{}, auth.User{}, true)
		Expect(err).To(BeNil())

		bRecord, err = dataProcessor.UpdateRecord(bMetaObj.Name, strconv.Itoa(int(bRecord["id"].(float64))), map[string]interface{}{"target": map[string]interface{}{"_object": cMetaObj.Name, "id": cRecord["id"]}}, auth.User{}, true)
		Expect(err).To(BeNil())
		Expect(bRecord["id"]).To(Equal(float64(1)))
		targetValue := bRecord["target"].(map[string]string)
		Expect(targetValue["_object"]).To(Equal(cMetaObj.Name))
		Expect(strconv.Atoi(targetValue["id"])).To(Equal(int(bRecord["id"].(float64))))
	})

	Describe("Retrieving records with generic values", func() {

		var aRecord map[string]interface{}
		var bRecord map[string]interface{}
		var err error

		havingObjectA := func() {
			By("having two objects: A and B")
			aMetaDescription := meta.MetaDescription{
				Name: "a",
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
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: false,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())
		}

		havingObjectBWithGenericLinkToA := func() {

			By("B contains generic inner field")

			bMetaDescription := meta.MetaDescription{
				Name: "b",
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
						Name:         "target",
						Type:         meta.FieldTypeGeneric,
						LinkType:     meta.LinkTypeInner,
						LinkMetaList: []string{"a"},
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
			aRecord, err = dataProcessor.CreateRecord("a", map[string]interface{}{"name": "A record"}, auth.User{}, true)
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectB := func() {
			bRecord, err = dataProcessor.CreateRecord("b", map[string]interface{}{"target": map[string]interface{}{"_object": "a", "id": aRecord["id"]}}, auth.User{}, true)
			Expect(err).To(BeNil())
		}

		It("can retrieve record containing generic inner value as a key", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having a record of object A", havingARecordOfObjectA)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectB)

			bRecord, err := dataProcessor.Get("b", strconv.Itoa(int(bRecord["id"].(float64))), 1)
			Expect(err).To(BeNil())
			targetValue := bRecord["target"].(map[string]string)
			Expect(targetValue["_object"]).To(Equal("a"))
			Expect(strconv.Atoi(targetValue["id"])).To(Equal(int(aRecord["id"].(float64))))
		})

		It("can retrieve record containing generic inner value as a full object", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having a record of object A", havingARecordOfObjectA)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectB)

			bRecord, err = dataProcessor.Get("b", strconv.Itoa(int(bRecord["id"].(float64))), 3)
			Expect(err).To(BeNil())
			targetValue := bRecord["target"].(map[string]interface{})
			Expect(targetValue["_object"]).To(Equal("a"))
			Expect(targetValue["id"].(float64)).To(Equal(aRecord["id"].(float64)))
			Expect(targetValue["name"].(string)).To(Equal(aRecord["name"]))
		})

		It("can retrieve record containing null generic inner value", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having a record of object A", havingARecordOfObjectA)

			bRecord, err = dataProcessor.CreateRecord("b", map[string]interface{}{}, auth.User{}, true)
			Expect(err).To(BeNil())

			bRecord, err = dataProcessor.Get("b", strconv.Itoa(int(bRecord["id"].(float64))), 3)
			Expect(err).To(BeNil())
			Expect(bRecord["target"]).To(BeNil())
		})
	})

	Describe("Querying records by generic fields` values", func() {

		var aRecord map[string]interface{}
		var bRecord map[string]interface{}
		var err error

		havingObjectA := func() {
			By("having two objects: A and B")
			aMetaDescription := meta.MetaDescription{
				Name: "a",
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
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: false,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())
		}

		havingObjectBWithGenericLinkToA := func() {

			By("B contains generic inner field")

			bMetaDescription := meta.MetaDescription{
				Name: "b",
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
						Name:         "target",
						Type:         meta.FieldTypeGeneric,
						LinkType:     meta.LinkTypeInner,
						LinkMetaList: []string{"a"},
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
			aRecord, err = dataProcessor.CreateRecord("a", map[string]interface{}{"name": "A record"}, auth.User{}, true)
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectA := func() {
			bRecord, err = dataProcessor.CreateRecord("b", map[string]interface{}{"target": map[string]interface{}{"_object": "a", "id": aRecord["id"]}}, auth.User{}, true)
			Expect(err).To(BeNil())
		}

		It("can retrieve record with generic field as key by querying by A record`s field", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having a record of object A", havingARecordOfObjectA)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			matchedRecords := []map[string]interface{}{}
			callbackFunction := func(obj map[string]interface{}) error {
				matchedRecords = append(matchedRecords, obj)
				return nil
			}

			err := dataProcessor.GetBulk("b", "eq(target.a.name,A%20record)", 1, callbackFunction)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
			targetValue := matchedRecords[0]["target"].(map[string]string)
			Expect(targetValue["_object"]).To(Equal("a"))
			Expect(strconv.Atoi(targetValue["id"])).To(Equal(int(aRecord["id"].(float64))))

		})

		It("can retrieve record with generic field as full object by querying by A record`s field", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having a record of object A", havingARecordOfObjectA)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			matchedRecords := []map[string]interface{}{}
			callbackFunction := func(obj map[string]interface{}) error {
				matchedRecords = append(matchedRecords, obj)
				return nil
			}

			err := dataProcessor.GetBulk("b", "eq(target.a.name,A%20record)", 2, callbackFunction)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
			targetValue := matchedRecords[0]["target"].(map[string]interface{})
			Expect(targetValue["_object"].(string)).To(Equal("a"))
			Expect(targetValue["id"].(float64)).To(Equal(aRecord["id"].(float64)))
			Expect(targetValue["name"].(string)).To(Equal(aRecord["name"].(string)))
		})
	})
})
