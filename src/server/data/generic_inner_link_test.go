package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/auth"
	"server/data"
	"server/data/record"
	"server/data/types"
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

	It("can create a record containing generic inner value", func() {
		By("having two objects: A and B")
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		By("B contains generic inner field")

		bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.AddField(&meta.Field{
			Name:         "target",
			Type:         meta.FieldTypeGeneric,
			LinkType:     meta.LinkTypeInner,
			LinkMetaList: []*meta.Meta{aMetaObj},
			Optional:     false,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(bMetaObj)

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
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		By("B contains generic inner field")

		bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.AddField(&meta.Field{
			Name:         "target",
			Type:         meta.FieldTypeGeneric,
			LinkType:     meta.LinkTypeInner,
			LinkMetaList: []*meta.Meta{aMetaObj},
			Optional:     false,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(bMetaObj)

		By("having a record of object B containing generic field value with A object`s record")
		_, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"target": map[string]interface{}{"_object": aMetaObj.Name, "id": 9999}}, auth.User{})
		Expect(err).To(Not(BeNil()))
		Expect(err.Error()).To(ContainSubstring("value does not exist"))
	})

	It("can update a record containing generic inner value", func() {
		By("having three objects: A, B and C")
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		aMetaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString, Optional: true})
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		cMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		cMetaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(cMetaObj)

		By("B contains generic inner field")

		bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.AddField(&meta.Field{
			Name:         "target",
			Type:         meta.FieldTypeGeneric,
			LinkType:     meta.LinkTypeInner,
			LinkMetaList: []*meta.Meta{aMetaObj, cMetaObj},
			Optional:     false,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(bMetaObj)

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
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		By("B contains generic inner field")

		bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.AddField(&meta.Field{
			Name:         "target",
			Type:         meta.FieldTypeGeneric,
			LinkType:     meta.LinkTypeInner,
			LinkMetaList: []*meta.Meta{aMetaObj},
			Optional:     true,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(bMetaObj)

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
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		cMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		cMetaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(cMetaObj)

		By("B contains generic inner field")

		bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.AddField(
			&meta.Field{
				Name: "target",
				Type: meta.FieldTypeGeneric,
				LinkType: meta.LinkTypeInner,
				LinkMetaList: []*meta.Meta{aMetaObj, cMetaObj},
				Optional:     false,
			},
			&meta.Field{Name: "name", Type: meta.FieldTypeString, Optional: false},
		)
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(bMetaObj)

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
		Expect(data["target"]).To(HaveKey(types.GenericInnerLinkObjectKey))
	})

	Describe("Retrieving records with generic values and casts PK value into its object PK type", func() {

		var aRecord *record.Record
		var bRecord *record.Record
		var err error

		havingObjectA := func() *meta.Meta {
			By("having two objects: A and B")
			aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			aMetaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString, Optional: false})
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMetaObj)
			return aMetaObj
		}

		havingObjectD := func(A *meta.Meta) *meta.Meta {
			By("having object D with ")
			dMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			dMetaDescription.AddField(
				&meta.Field{Name: "a", Type: meta.FieldTypeObject, LinkType: meta.LinkTypeInner, LinkMeta: A},
			)
			dMetaObj, err := metaStore.NewMeta(dMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(dMetaObj)

			return dMetaObj
		}

		havingObjectAWithOuterLinkToD := func(D *meta.Meta) {
			By("having object A with outer link to D")
			aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			aMetaDescription.AddField(
				&meta.Field{Name: "name", Type: meta.FieldTypeString, Optional: false},
				&meta.Field{
					Name:           "d_set",
					Type:           meta.FieldTypeArray,
					LinkType:       meta.LinkTypeOuter,
					LinkMeta:       D,
					OuterLinkField: D.FindField("a"),
					RetrieveMode:   true,
					Optional:       true,
				},
			)
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Update(aMetaObj)
		}

		havingObjectBWithGenericLinkToA := func(A *meta.Meta) *meta.Meta {

			By("B contains generic inner field")

			bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField(&meta.Field{
				Name:         "target",
				Type:         meta.FieldTypeGeneric,
				LinkType:     meta.LinkTypeInner,
				LinkMetaList: []*meta.Meta{A},
				Optional:     true,
			})
			bMetaObj, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())

			return metaStore.Create(bMetaObj)
		}

		havingObjectCWithGenericLinkToB := func(B *meta.Meta) {

			By("C contains generic inner field")

			cMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			cMetaDescription.AddField(&meta.Field{
				Name:         "target",
				Type:         meta.FieldTypeGeneric,
				LinkType:     meta.LinkTypeInner,
				LinkMetaList: []*meta.Meta{B},
				Optional:     true,
			})
			cMetaObj, err := metaStore.NewMeta(cMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(cMetaObj)
		}

		havingARecordOfObjectA := func() {
			aRecord, err = dataProcessor.CreateRecord("a", map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectB := func() {
			bRecord, err = dataProcessor.CreateRecord("b", map[string]interface{}{"target": map[string]interface{}{"_object": "a", "id": aRecord.Data["id"]}}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("can retrieve record containing generic inner value as a key", func() {

			aMeta := havingObjectA()
			havingObjectBWithGenericLinkToA(aMeta)
			havingARecordOfObjectA()

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectB)

			bRecord, err := dataProcessor.Get("b", strconv.Itoa(int(bRecord.Data["id"].(float64))), nil, nil, 1, false)
			Expect(err).To(BeNil())
			data := bRecord.GetData()
			targetValue := data["target"].(map[string]interface{})
			Expect(targetValue["_object"]).To(Equal("a"))
			value, ok := targetValue["id"].(float64)
			Expect(ok).To(BeTrue())
			Expect(value).To(Equal(aRecord.Data["id"].(float64)))
		})

		It("can retrieve record containing generic inner value as a full object", func() {

			aMeta := havingObjectA()
			havingObjectBWithGenericLinkToA(aMeta)
			havingARecordOfObjectA()

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectB)

			bRecord, err := dataProcessor.Get("b", strconv.Itoa(int(bRecord.Data["id"].(float64))), nil, nil, 3, false)
			Expect(err).To(BeNil())
			targetValue := bRecord.Data["target"].(*record.Record)
			Expect(targetValue.Data["_object"]).To(Equal("a"))
			Expect(targetValue.Data["id"].(float64)).To(Equal(aRecord.Data["id"].(float64)))
			Expect(targetValue.Data["name"].(string)).To(Equal(aRecord.Data["name"]))
		})

		It("can retrieve record containing nested generic relations", func() {

			aMeta := havingObjectA()
			bMeta := havingObjectBWithGenericLinkToA(aMeta)
			havingObjectCWithGenericLinkToB(bMeta)

			Describe("And having a record of object A", havingARecordOfObjectA)

			bRecord, err = dataProcessor.CreateRecord("b", map[string]interface{}{
				"target": map[string]interface{}{"_object": "a", "id": aRecord.Data["id"]},
			}, auth.User{})
			Expect(err).To(BeNil())

			cRecord, err := dataProcessor.CreateRecord("c", map[string]interface{}{
				"target": map[string]interface{}{"_object": "b", "id": bRecord.Data["id"]},
			}, auth.User{})
			Expect(err).To(BeNil())

			bRecord, err := dataProcessor.Get("c", strconv.Itoa(int(cRecord.Data["id"].(float64))), nil, nil, 3, false)
			Expect(err).To(BeNil())
			data := bRecord.GetData()
			Expect(data["target"].(map[string]interface{})["_object"].(string)).To(Equal("b"))
			Expect(data["target"].(map[string]interface{})["target"].(map[string]interface{})["name"].(string)).To(Equal("A record"))
		})

		It("can retrieve record containing null generic inner value", func() {
			aMeta := havingObjectA()
			havingObjectBWithGenericLinkToA(aMeta)
			havingARecordOfObjectA()

			bRecord, err = dataProcessor.CreateRecord("b", map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			bRecord, err := dataProcessor.Get("b", strconv.Itoa(int(bRecord.Data["id"].(float64))), nil, nil, 3, false)
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("target"))
			Expect(bRecord.Data["target"]).To(BeNil())
		})

		It("can retrieve record with generic value respecting specified depth", func() {
			aMeta := havingObjectA()
			dMeta := havingObjectD(aMeta)
			havingObjectAWithOuterLinkToD(dMeta)
			havingObjectBWithGenericLinkToA(aMeta)
			havingARecordOfObjectA()

			_, err := dataProcessor.CreateRecord(
				"d", map[string]interface{}{"a": aRecord.Pk()}, auth.User{},
			)
			Expect(err).To(BeNil())

			bRecord, err = dataProcessor.CreateRecord("b", map[string]interface{}{
				"target": map[string]interface{}{"_object": "a", "id": aRecord.Data["id"]},
			}, auth.User{})
			Expect(err).To(BeNil())

			bRecord, err = dataProcessor.Get("b", strconv.Itoa(int(bRecord.Data["id"].(float64))), nil, nil, 2, false)
			Expect(err).To(BeNil())

			_, ok := bRecord.Data["target"].(*record.Record).Data["d_set"].([]interface{})
			Expect(ok).To(BeTrue())
		})
	})

	Describe("Querying records by generic fields` values", func() {

		var aRecord *record.Record
		var bRecord *record.Record
		var cRecord *record.Record
		var err error

		havingObjectA := func() *meta.Meta {
			aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			aMetaDescription.AddField(&meta.Field{
				Name:     "name",
				Type:     meta.FieldTypeString,
				Optional: false,
			})
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())

			return metaStore.Create(aMetaObj)
		}

		havingObjectC := func() *meta.Meta {
			cMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			cMetaObj, err := metaStore.NewMeta(cMetaDescription)
			Expect(err).To(BeNil())

			return metaStore.Create(cMetaObj)
		}

		havingObjectBWithGenericLinkToAAndC := func(A, C *meta.Meta) {

			By("B contains generic inner field")

			bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField(&meta.Field{
				Name:         "target",
				Type:         meta.FieldTypeGeneric,
				LinkType:     meta.LinkTypeInner,
				LinkMetaList: []*meta.Meta{A, C},
				Optional:     true,
			})
			bMetaObj, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMetaObj)
		}

		havingARecordOfObjectA := func() {
			aRecord, err = dataProcessor.CreateRecord("a", map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectC := func() {
			cRecord, err = dataProcessor.CreateRecord("c", map[string]interface{}{"name": "C record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectA := func() {
			bRecord, err = dataProcessor.CreateRecord("b", map[string]interface{}{"target": map[string]interface{}{"_object": "a", "id": aRecord.Data["id"]}}, auth.User{})
			Expect(err).To(BeNil())
		}

		havingARecordOfObjectBContainingRecordOfObjectC := func() {
			bRecord, err = dataProcessor.CreateRecord("b", map[string]interface{}{"target": map[string]interface{}{"_object": "c", "id": cRecord.Data["id"]}}, auth.User{})
			Expect(err).To(BeNil())
		}

		It("can retrieve record with generic field as key by querying by A record`s field", func() {

			aMeta := havingObjectA()
			cMeta := havingObjectC()
			havingObjectBWithGenericLinkToAAndC(aMeta, cMeta)
			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("And having a record of object C", havingARecordOfObjectC)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)
			Describe("and having a record of object B containing null generic field value ", havingARecordOfObjectBContainingRecordOfObjectC)

			_, matchedRecords, err := dataProcessor.GetBulk("b", "eq(target.a.name,A%20record)", nil, nil, 1, false)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
			targetValue := matchedRecords[0].Data["target"].(*types.GenericInnerLink).AsMap()
			Expect(targetValue["_object"]).To(Equal("a"))
			Expect(targetValue["id"].(float64)).To(Equal(aRecord.Data["id"].(float64)))

		})

		It("can retrieve record with generic field as full object by querying by A record`s field", func() {

			aMeta := havingObjectA()
			cMeta := havingObjectC()
			havingObjectBWithGenericLinkToAAndC(aMeta, cMeta)
			Describe("And having a record of object A", havingARecordOfObjectA)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			_, matchedRecords, err := dataProcessor.GetBulk("b", "eq(target.a.name,A%20record)", nil, nil, 2, false)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
			targetValue := matchedRecords[0].Data["target"].(*record.Record)
			Expect(targetValue.Data["_object"].(string)).To(Equal("a"))
			Expect(targetValue.Data["id"].(float64)).To(Equal(aRecord.Data["id"].(float64)))
			Expect(targetValue.Data["name"].(string)).To(Equal(aRecord.Data["name"].(string)))
		})

		It("can query records by generic_field's type", func() {

			aMeta := havingObjectA()
			cMeta := havingObjectC()
			havingObjectBWithGenericLinkToAAndC(aMeta, cMeta)
			Describe("And having a record of object A", havingARecordOfObjectA)

			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			_, err = dataProcessor.CreateRecord("b", map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())

			_, matchedRecords, err := dataProcessor.GetBulk("b", "eq(target._object,a)", nil, nil, 2, false)
			Expect(err).To(BeNil())
			Expect(matchedRecords).To(HaveLen(1))
			targetValue := matchedRecords[0].Data["target"].(*record.Record)
			Expect(targetValue.Data["_object"].(string)).To(Equal("a"))
			Expect(targetValue.Data["id"].(float64)).To(Equal(aRecord.Data["id"].(float64)))
			Expect(targetValue.Data["name"].(string)).To(Equal(aRecord.Data["name"].(string)))
		})

		It("can create record with nested new inner generic record", func() {

			aMeta := havingObjectA()
			cMeta := havingObjectC()
			havingObjectBWithGenericLinkToAAndC(aMeta, cMeta)

			bRecordData := map[string]interface{}{"target": map[string]interface{}{"_object": "a", "name": "Some A record"}}

			bRecord, err = dataProcessor.CreateRecord("b", bRecordData, auth.User{})
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("target"))
			Expect(bRecord.Data["target"]).To(Not(BeNil()))
		})

		It("can create record with nested existing inner generic record", func() {

			aMeta := havingObjectA()
			cMeta := havingObjectC()
			havingObjectBWithGenericLinkToAAndC(aMeta, cMeta)

			existingARecord, err := dataProcessor.CreateRecord(
				"a",
				map[string]interface{}{"name": "Existing A record"},
				auth.User{},
			)
			Expect(err).To(BeNil())

			bRecordData := map[string]interface{}{"target": map[string]interface{}{"_object": "a", "id": existingARecord.Data["id"]}}

			bRecord, err = dataProcessor.CreateRecord("b", bRecordData, auth.User{})
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("target"))
			Expect(bRecord.Data["target"]).To(Not(BeNil()))
			Expect(bRecord.Data["target"].(map[string]interface{})["id"]).To(Equal(existingARecord.Data["id"]))
		})
	})
})
