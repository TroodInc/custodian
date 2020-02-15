package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/errors"
	"server/object/driver"
	"server/object/meta"
	"utils"
)

var _ = Describe("The PG MetaStore", func() {
	appConfig := utils.GetConfig()

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := NewStore(driver)

	AfterEach(func() {
		metaStore.Flush()
	})

	It("can flush all objects", func() {
		Context("once object is created", func() {
			meta, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
			Expect(err).To(BeNil())
			metaStore.Create(meta)

			Context("and 'flush' method is called", func() {
				metaStore.Flush()

				metaList := metaStore.List()
				Expect(metaList).To(HaveLen(0))
			})
		})
	})

	It("can remove object without leaving orphan outer links", func() {
		Context("having two objects with mutual links", func() {
			aMetaDescription := GetBaseMetaData(utils.RandomString(8))
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMeta)

			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField(&meta.Field{
				Name:     "a_fk",
				Type:     meta.FieldTypeObject,
				Optional: true,
				LinkType: meta.LinkTypeInner,
				LinkMeta: aMeta,
			})
			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)

			aMetaDescription.AddField(&meta.Field{
				Name:           "b_set",
				Type:           meta.FieldTypeObject,
				Optional:       true,
				LinkType:       meta.LinkTypeOuter,
				LinkMeta:       bMeta,
				OuterLinkField: bMeta.FindField("a_fk"),
			})
			aMeta, err = metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Update(aMeta)

			Context("and 'remove' method is called for B meta", func() {
				metaStore.Remove(bMeta.Name)
				Context("meta A should not contain outer link field which references B meta", func() {
					aMeta = metaStore.Get(aMeta.Name)
					Expect(aMeta.Fields).To(HaveLen(1))
					Expect(aMeta.Fields).To(HaveKey("id"))
				})

			})
		})
	})

	It("can remove object without leaving orphan inner links", func() {
		Context("having two objects with mutual links", func() {
			aMetaDescription := GetBaseMetaData(utils.RandomString(8))
			aMeta, err := metaStore.NewMeta(aMetaDescription)/*
			Expect(err).To(BeNil())*/
			metaStore.Create(aMeta)

			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField( &meta.Field{
				Name:     "a_fk",
				Type:     meta.FieldTypeObject,
				Optional: true,
				LinkType: meta.LinkTypeInner,
				LinkMeta: aMeta,
			})
			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			Context("and 'remove' method is called for meta A", func() {
				metaStore.Remove(aMeta.Name)

				Context("meta B should not contain inner link field which references A meta", func() {
					bMeta = metaStore.Get(bMeta.Name)
					Expect(bMeta.Fields).To(HaveLen(1))
					Expect(bMeta.Fields).To(HaveKey("id"))
				})
			})
		})
	})

	It("can remove object`s inner link field without leaving orphan outer links", func() {
		Context("having objects A and B with mutual links", func() {
			aMetaDescription := GetBaseMetaData(utils.RandomString(8))
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMeta)

			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField( &meta.Field{
				Name:     "a_fk",
				Type:     meta.FieldTypeObject,
				Optional: true,
				LinkType: meta.LinkTypeInner,
				LinkMeta: aMeta,
			})
			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			aMetaDescription.AddField( &meta.Field{
				Name:           "b_set",
				Type:           meta.FieldTypeObject,
				Optional:       true,
				LinkType:       meta.LinkTypeOuter,
				LinkMeta:       bMeta,
				OuterLinkField: bMeta.FindField("a_fk"),
			})
			aMeta, err = metaStore.NewMeta(aMetaDescription)
			metaStore.Update(aMeta)
			Expect(err).To(BeNil())

			Context("and inner link field was removed from object B", func() {
				bMetaDescription := GetBaseMetaData(bMetaDescription.Name)
				bMeta, err := metaStore.NewMeta(bMetaDescription)
				Expect(err).To(BeNil())
				metaStore.Update(bMeta)

				Context("outer link field should be removed from object A", func() {
					aMeta = metaStore.Get(aMeta.Name)
					Expect(err).To(BeNil())
					Expect(aMeta.Fields).To(HaveLen(1))
					Expect(aMeta.Fields).To(HaveKey("id"))
				})
			})
		})
	})

	It("checks object for fields with duplicated names when creating object", func() {
		Context("having an object description with duplicated field names", func() {
			metaDescription := GetBaseMetaData(utils.RandomString(8))
			metaDescription.AddField([]*meta.Field{
				{
					Name:     "name",
					Type:     meta.FieldTypeString,
					Optional: false,
				}, {
					Name:     "name",
					Type:     meta.FieldTypeString,
					Optional: true,
				},
			}...)
			Context("When 'NewMeta' method is called it should return error", func() {
				_, err := metaStore.NewMeta(metaDescription)
				Expect(err).To(Not(BeNil()))
				Expect(err).To(Equal(
					errors.NewValidationError("", "Object contains duplicated field 'name'", nil)),
				)
			})
		})
	})

	It("can change field type of existing object", func() {
		By("having an existing object with string field")
		metaDescription := GetBaseMetaData(utils.RandomString(8))
		metaDescription.AddField( &meta.Field{
			Name:     "name",
			Type:     meta.FieldTypeNumber,
			Optional: false,
		})
		metaObj, err := metaStore.NewMeta(metaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(metaObj)
	})

	It("creates inner link with 'on_delete' behavior defined as 'CASCADE' by default", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B referencing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField( &meta.Field{
				Name:     "a",
				LinkMeta: aMetaDescription,
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				Optional: false,
			})
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMeta)

			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)

			//assert meta
			bMeta = metaStore.Get(bMeta.Name)
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(meta.OnDeleteCascade))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'CASCADE' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField( &meta.Field{
				Name:     "a",
				LinkMeta: aMetaDescription,
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				OnDelete: "cascade",
				Optional: false,
			})
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMeta)

			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)

			//assert meta
			bMeta = metaStore.Get(bMeta.Name)
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(meta.OnDeleteCascade))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'SET NULL' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField( &meta.Field{
				Name:     "a",
				LinkMeta: aMetaDescription,
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				OnDelete: "setNull",
				Optional: false,
			})
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMeta)

			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)
			//assert meta
			bMeta = metaStore.Get(bMeta.Name)
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(meta.OnDeleteSetNull))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'RESTRICT' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField( &meta.Field{
				Name:     "a",
				LinkMeta: aMetaDescription,
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				OnDelete: "restrict",
				Optional: false,
			})
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMeta)

			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)

			//assert meta
			bMeta = metaStore.Get(bMeta.Name)
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(meta.OnDeleteRestrict))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'RESTRICT' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField( &meta.Field{
				Name:     "a",
				LinkMeta: aMetaDescription,
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				OnDelete: "setDefault",
				Optional: false,
			})
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMeta)

			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)

			//assert meta
			bMeta = metaStore.Get(bMeta.Name)
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(meta.OnDeleteSetDefault))
			Expect(err).To(BeNil())
		})
	})
})
