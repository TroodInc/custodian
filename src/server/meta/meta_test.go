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
			meta, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(meta)
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
					},
				},
			}
			aMeta, _ := metaStore.NewMeta(&aMetaDescription)
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
			metaStore.Update(aMeta.Name, aMeta)
			Expect(err).To(BeNil())

			Context("and 'remove' method is called", func() {
				metaStore.Remove(bMeta.Name, true)
				aMeta, _, _ = metaStore.Get(aMeta.Name)
				Expect(aMeta.Fields).To(HaveLen(1))
				Expect(aMeta.Fields[0].Name).To(Equal("id"))
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

	It("checks object for fields with inconsistent configuration", func() {
		Context("having an object description with both 'optional' and 'default' specified", func() {
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
						Optional: false,
					},
				},
			}
			Context("When 'NewMeta' method is called it should return error", func() {
				_, err := metaStore.NewMeta(&metaDescription)
				Expect(err).To(Not(BeNil()))
				Expect(err.Error()).To(Equal("Mandatory field 'id' cannot have default value"))
			})
		})
	})
})
