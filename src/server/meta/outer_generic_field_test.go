package meta_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"server/meta"
	"utils"
)

var _ = Describe("Outer generic field", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	It("can create object with outer generic field", func() {
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
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

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
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("and object C, containing generic inner field")
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
				},
				{
					Name:         "target",
					Type:         meta.FieldTypeGeneric,
					LinkType:     meta.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name, bMetaObj.Name},
					Optional:     false,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
		aMetaDescription = meta.MetaDescription{
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
				},
				{
					Name:           "c_set",
					Type:           meta.FieldTypeGeneric,
					LinkType:       meta.LinkTypeOuter,
					LinkMeta:       "c",
					OuterLinkField: "target",
				},
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true, true)
		Expect(err).To(BeNil())

		// check meta fields
		aMeta, _, err := metaStore.Get(aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.Fields[1].Name).To(Equal("c_set"))
		Expect(aMeta.Fields[1].LinkMeta.Name).To(Equal("c"))
	})

	It("Detects non-existing linked meta", func() {
		By("having an object A, referencing non-existing object B")

		cMetaDescription := meta.MetaDescription{
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
				},
				{
					Name:           "target",
					Type:           meta.FieldTypeGeneric,
					LinkType:       meta.LinkTypeOuter,
					LinkMeta:       "b",
					OuterLinkField: "some_field",
				},
			},
		}
		By("Meta should not be created")
		_, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(Not(BeNil()))
	})

	It("Fails if OuterLinkField not specified", func() {
		By("having object A")
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
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("and object B, containing generic inner field")
		cMetaDescription := meta.MetaDescription{
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
		metaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
		aMetaDescription = meta.MetaDescription{
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
				},
				{
					Name:     "b_set",
					Type:     meta.FieldTypeGeneric,
					LinkType: meta.LinkTypeOuter,
					LinkMeta: "b",
				},
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(Not(BeNil()))

	})

	It("can remove generic field from object", func() {
		By("having object A")
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
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("and object B, containing generic inner field")
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

		By("and outer generic field added to object A")
		aMetaDescription = meta.MetaDescription{
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
				},
				{
					Name:           "b_set",
					Type:           meta.FieldTypeGeneric,
					LinkType:       meta.LinkTypeOuter,
					LinkMeta:       "b",
					OuterLinkField: "target",
				},
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(bMetaObj.Name, bMetaObj, true, true)
		Expect(err).To(BeNil())

		By("and outer generic field removed from object A")
		aMetaDescription = meta.MetaDescription{
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
				},
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(bMetaObj.Name, bMetaObj, true, true)
		Expect(err).To(BeNil())

	})

	It("removes outer field if corresponding inner field is removed", func() {
		By("having object A")
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
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("and object B, containing generic inner field")
		cMetaDescription := meta.MetaDescription{
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
		cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(cMetaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
		aMetaDescription = meta.MetaDescription{
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
				},
				{
					Name:           "b_set",
					Type:           meta.FieldTypeGeneric,
					LinkType:       meta.LinkTypeOuter,
					LinkMeta:       "b",
					OuterLinkField: "target",
				},
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true, true)
		Expect(err).To(BeNil())

		//

		By("and inner generic field removed from object B")
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
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(bMetaObj.Name, bMetaObj, true, true)
		Expect(err).To(BeNil())

		By("outer link should be removed from object A")
		// check meta fields
		aMetaObj, _, err = metaStore.Get(aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMetaObj.Fields).To(HaveLen(1))
		Expect(aMetaObj.Fields[0].Name).To(Equal("id"))

	})

	It("removes outer field if object containing corresponding inner field is removed", func() {
		By("having object A")
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
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("and object B, containing generic inner field")
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

		By("and outer generic field added to object A")
		aMetaDescription = meta.MetaDescription{
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
				},
				{
					Name:           "b_set",
					Type:           meta.FieldTypeGeneric,
					LinkType:       meta.LinkTypeOuter,
					LinkMeta:       "b",
					OuterLinkField: "target",
				},
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true, true)
		Expect(err).To(BeNil())

		//

		By("and object B is removed")

		_, err = metaStore.Remove(bMetaObj.Name, true, true)
		Expect(err).To(BeNil())

		By("outer link should be removed from object A")
		// check meta fields
		aMetaObj, _, err = metaStore.Get(aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMetaObj.Fields).To(HaveLen(1))
		Expect(aMetaObj.Fields[0].Name).To(Equal("id"))

	})

	It("does not remove outer field for object if it was not specified in object's description", func() {
		By("having object A")
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
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("and object B, containing generic inner field")
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

		By("and object A has been updated with data, which does not have outer generic field")
		aMetaDescription = meta.MetaDescription{
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
				},
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true, true)
		Expect(err).To(BeNil())
		//

		aMetaObj, _, err = metaStore.Get(aMetaDescription.Name, true)
		Expect(err).To(BeNil())

		Expect(aMetaObj.Fields).To(HaveLen(2))

	})
})
