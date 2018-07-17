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

var _ = FDescribe("Data", func() {
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
		aRecord, err := dataProcessor.Put(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		By("and having a record of object B containing generic field value with A object`s record")
		bRecord, err := dataProcessor.Put(bMetaObj.Name, map[string]interface{}{"target": map[string]interface{}{"_object": aMetaObj.Name, "pk": aRecord["id"]}}, auth.User{})
		Expect(err).To(BeNil())
		Expect(bRecord["id"]).To(Equal(float64(1)))
		targetValue := bRecord["target"].(map[string]string)
		Expect(targetValue["_object"]).To(Equal(aMetaObj.Name))
		Expect(strconv.Atoi(targetValue["pk"])).To(Equal(int(aRecord["id"].(float64))))
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
		aRecord, err := dataProcessor.Put(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		By("and having a record of object B containing generic field value with A object`s record")
		bRecord, err := dataProcessor.Put(bMetaObj.Name, map[string]interface{}{"target": map[string]interface{}{"_object": aMetaObj.Name, "pk": aRecord["id"]}}, auth.User{})
		Expect(err).To(BeNil())

		By("this record is updated with record of object C")

		cRecord, err := dataProcessor.Put(cMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err = dataProcessor.Update(bMetaObj.Name, strconv.Itoa(int(bRecord["id"].(float64))), map[string]interface{}{"target": map[string]interface{}{"_object": cMetaObj.Name, "pk": cRecord["id"]}}, auth.User{})
		Expect(err).To(BeNil())
		Expect(bRecord["id"]).To(Equal(float64(1)))
		targetValue := bRecord["target"].(map[string]string)
		Expect(targetValue["_object"]).To(Equal(cMetaObj.Name))
		Expect(strconv.Atoi(targetValue["pk"])).To(Equal(int(bRecord["id"].(float64))))

	})
})
