package meta_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"server/meta"
)

var _ = Describe("The PG MetaStore", func() {
	databaseConnectionOptions := "host=localhost dbname=custodian sslmode=disable"
	syncer, _ := pg.NewSyncer(databaseConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	It("can flush all objects", func() {
		Context("once object is created", func() {
			metaDescription := meta.MetaDescription{
				Name: "person",
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
					}, {
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: false,
					}, {
						Name:     "gender",
						Type:     meta.FieldTypeString,
						Optional: true,
					},
				},
			}
			meta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(meta)
			Expect(err).To(BeNil())
			Context("and 'flush' method is called", func() {
				metaStore.Flush()
				metaList, _, _ := metaStore.List()
				Expect(*metaList).To(HaveLen(0))
			})
		})
	})

	It("can remove object without leaving orphan outer links", func() {
		Context("having two objects with mutual links", func() {
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
			aMeta, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMeta)
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
						Optional: true,
					},
					{
						Name:     "a_fk",
						Type:     meta.FieldTypeObject,
						Optional: true,
						LinkType: meta.LinkTypeInner,
						LinkMeta: "a",
					},
				},
			}
			bMeta, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMeta)
			Expect(err).To(BeNil())

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
						Optional: true,
					},
					{
						Name:           "b_set",
						Type:           meta.FieldTypeObject,
						Optional:       true,
						LinkType:       meta.LinkTypeOuter,
						LinkMeta:       "b",
						OuterLinkField: "a_fk",
					},
				},
			}
			aMeta, err = metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Update(aMeta.Name, aMeta, true)

			Context("and 'remove' method is called for B meta", func() {
				metaStore.Remove(bMeta.Name, true, true)
				Context("meta A should not contain outer link field which references B meta", func() {
					aMeta, _, _ = metaStore.Get(aMeta.Name, true)
					Expect(aMeta.Fields).To(HaveLen(1))
					Expect(aMeta.Fields[0].Name).To(Equal("id"))
				})

			})
		})
	})

	It("can remove object without leaving orphan inner links", func() {
		Context("having two objects with mutual links", func() {
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
			aMeta, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMeta)

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
						Name:     "a_fk",
						Type:     meta.FieldTypeObject,
						Optional: true,
						LinkType: meta.LinkTypeInner,
						LinkMeta: "a",
					},
				},
			}
			bMeta, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			Context("and 'remove' method is called for meta A", func() {
				metaStore.Remove(aMeta.Name, true, true)

				Context("meta B should not contain inner link field which references A meta", func() {
					bMeta, _, _ = metaStore.Get(bMeta.Name, true)
					Expect(bMeta.Fields).To(HaveLen(1))
					Expect(bMeta.Fields[0].Name).To(Equal("id"))
				})
			})
		})
	})

	It("can remove object`s inner link field without leaving orphan outer links", func() {
		Context("having objects A and B with mutual links", func() {
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
			aMeta, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMeta)

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
						Name:     "a_fk",
						Type:     meta.FieldTypeObject,
						Optional: true,
						LinkType: meta.LinkTypeInner,
						LinkMeta: "a",
					},
				},
			}
			bMeta, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)
			Expect(err).To(BeNil())

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
						Optional: true,
					},
					{
						Name:           "b_set",
						Type:           meta.FieldTypeObject,
						Optional:       true,
						LinkType:       meta.LinkTypeOuter,
						LinkMeta:       "b",
						OuterLinkField: "a_fk",
					},
				},
			}
			aMeta, err = metaStore.NewMeta(&aMetaDescription)
			metaStore.Update(aMeta.Name, aMeta, true)
			Expect(err).To(BeNil())

			Context("and inner link field was removed from object B", func() {
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
					},
				}
				bMeta, err := metaStore.NewMeta(&bMetaDescription)
				Expect(err).To(BeNil())
				metaStore.Update(bMeta.Name, bMeta,true)

				Context("outer link field should be removed from object A", func() {
					aMeta, _, err = metaStore.Get(aMeta.Name,true)
					Expect(err).To(BeNil())
					Expect(aMeta.Fields).To(HaveLen(1))
					Expect(aMeta.Fields[0].Name).To(Equal("id"))
				})
			})
		})
	})

	It("checks object for fields with duplicated names when creating object", func() {
		Context("having an object description with duplicated field names", func() {
			metaDescription := meta.MetaDescription{
				Name: "person",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					}, {
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: false,
					}, {
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: true,
					},
				},
			}
			Context("When 'NewMeta' method is called it should return error", func() {
				_, err := metaStore.NewMeta(&metaDescription)
				Expect(err).To(Not(BeNil()))
				Expect(err.Error()).To(Equal("Object contains duplicated field 'name'"))
			})
		})
	})
})
