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
	"server/data/types"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := object.NewStore(object.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	Describe("Querying records by generic fields` values", func() {

		var aRecord map[string]interface{}
		var bRecord map[string]interface{}
		var err error

		havingObjectA := func() {
			aMetaDescription := object.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
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
			bMetaDescription := object.MetaDescription{
				Name: "b",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name: "id",
						Type: object.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
					{
						Name:         "target",
						Type:         object.FieldTypeGeneric,
						LinkType:     object.LinkTypeInner,
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

		havingObjectAWithGenericOuterLinkToB := func() {
			aMetaDescription := object.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
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
						Optional: false,
					},
					{
						Name:           "b_set",
						Type:           object.FieldTypeGeneric,
						LinkType:       object.LinkTypeOuter,
						LinkMeta:       "b",
						OuterLinkField: "target",
						Optional:       true,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true, true)
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

		It("can retrieve record with outer generic field as key", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having object A containing outer field referencing object B", havingObjectAWithGenericOuterLinkToB)
			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			aRecord, err = dataProcessor.Get("a", strconv.Itoa(int(aRecord["id"].(float64))), 1, true)
			Expect(err).To(BeNil())
			bSet := aRecord["b_set"].([]interface{})
			Expect(bSet).To(HaveLen(1))
			Expect(bSet).To(Equal([]interface{}{bRecord["id"].(float64)}))
		})

		It("can retrieve record with outer generic field as object", func() {

			Describe("Having object A", havingObjectA)
			Describe("And having object B", havingObjectBWithGenericLinkToA)
			Describe("And having object A containing outer field referencing object B", havingObjectAWithGenericOuterLinkToB)
			Describe("And having a record of object A", havingARecordOfObjectA)
			Describe("and having a record of object B containing generic field value with A object`s record", havingARecordOfObjectBContainingRecordOfObjectA)

			aRecord, err = dataProcessor.Get("a", strconv.Itoa(int(aRecord["id"].(float64))), 3, true)
			Expect(err).To(BeNil())
			bSet := aRecord["b_set"].([]interface{})
			Expect(bSet).To(HaveLen(1))
			targetValue := bSet[0].(map[string]interface{})["target"].(map[string]interface{})
			Expect(targetValue[types.GenericInnerLinkObjectKey].(string)).To(Equal("a"))
		})
	})
})
