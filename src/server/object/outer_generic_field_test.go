package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/driver"
	"server/object/meta"
	"utils"
)

var _ = Describe("Outer generic field", func() {
	appConfig := utils.GetConfig()

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := NewStore(driver)

	AfterEach(func() {
		metaStore.Flush()
	})

	It("can create object with manually specified outer generic field", func() {
		By("having two objects: A and B")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(bMetaObj)

		By("and object C, containing generic inner field")
		cMetaDescription := GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.AddField(&meta.Field{
			Name:         "target",
			Type:         meta.FieldTypeGeneric,
			LinkType:     meta.LinkTypeInner,
			LinkMetaList: []*meta.Meta{aMetaObj, bMetaObj},
			Optional:     false,
		})
		cMetaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(cMetaObj)

		By("and outer generic field added to object A")
		aMetaDescription.AddField(&meta.Field{
			Name:           "c_set",
			Type:           meta.FieldTypeGeneric,
			LinkType:       meta.LinkTypeOuter,
			LinkMeta:       cMetaObj,
			OuterLinkField: cMetaObj.FindField("target"),
		})

		(&meta.NormalizationService{}).Normalize(aMetaDescription)
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Update(aMetaObj)

		// check meta fields
		aMeta := metaStore.Get(aMetaDescription.Name)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.Fields).To(HaveKey("c_set"))
		Expect(aMeta.Fields["c_set"].LinkMeta.Name).To(Equal(cMetaObj.Name))
		Expect(aMeta.Fields["c_set"].QueryMode).To(BeTrue())
		Expect(aMeta.Fields["c_set"].RetrieveMode).To(BeTrue())
	})

	It("Detects non-existing linked meta", func() {
		By("having an object A, referencing non-existing object B")
		bMeta := GetBaseMetaData(utils.RandomString(8))
		cMetaDescription := GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.AddField(&meta.Field{
			Name:           "target",
			Type:           meta.FieldTypeGeneric,
			LinkType:       meta.LinkTypeOuter,
			LinkMeta:       bMeta,
			OuterLinkField: bMeta.FindField("some_field"),
		})
		By("MetaDescription should not be created")
		_, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(Not(BeNil()))
	})

	It("Fails if OuterLinkField not specified", func() {
		By("having object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		By("and object B, containing generic inner field")
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.AddField(&meta.Field{
			Name:         "target",
			Type:         meta.FieldTypeGeneric,
			LinkType:     meta.LinkTypeInner,
			LinkMetaList: []*meta.Meta{aMetaObj},
			Optional:     false,
		})
		metaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(metaObj)

		By("and outer generic field added to object A")
		aMetaDescription = GetBaseMetaData(utils.RandomString(8))
		aMetaDescription.AddField(&meta.Field{
			Name:     "b_set",
			Type:     meta.FieldTypeGeneric,
			LinkType: meta.LinkTypeOuter,
			LinkMeta: metaObj,
		})
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(Not(BeNil()))

	})

	It("can remove outer generic field from object", func() {
		By("having object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		By("and object B, containing generic inner field")
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
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

		By("and outer generic field added to object A")
		aMetaDescription.AddField(&meta.Field{
			Name:           "b_set",
			Type:           meta.FieldTypeGeneric,
			LinkType:       meta.LinkTypeOuter,
			LinkMeta:       bMetaObj,
			OuterLinkField: bMetaObj.FindField("target"),
		})
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Update(bMetaObj)
		Expect(err).To(BeNil())

		By("and outer generic field removed from object A")
		aMetaDescription = GetBaseMetaData(aMetaObj.Name)
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Update(bMetaObj)
		Expect(err).To(BeNil())
	})

	It("removes outer generic field if corresponding inner generic field is removed", func() {
		By("having object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		By("and object B, containing generic inner field")
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
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

		By("and outer generic field added to object A")
		aMetaDescription.AddField(&meta.Field{
			Name:           "b_set",
			Type:           meta.FieldTypeGeneric,
			LinkType:       meta.LinkTypeOuter,
			LinkMeta:       bMetaObj,
			OuterLinkField: bMetaObj.FindField("target"),
		})
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Update(aMetaObj)
		//

		By("and inner generic field removed from object B")
		bMetaDescription = GetBaseMetaData(bMetaObj.Name)
		bMetaObj, err = metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Update(bMetaObj)

		By("outer link should be removed from object A")
		// check meta fields
		aMetaObj = metaStore.Get(aMetaDescription.Name)
		Expect(err).To(BeNil())
		Expect(aMetaObj.Fields).To(HaveLen(1))
		Expect(aMetaObj.Fields).To(HaveKey("id"))

	})

	It("removes outer field if object containing corresponding inner field is removed", func() {
		By("having object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		By("and object B, containing generic inner field")
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
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

		By("and outer generic field added to object A")
		aMetaDescription.AddField(&meta.Field{
			Name:           "b_set",
			Type:           meta.FieldTypeGeneric,
			LinkType:       meta.LinkTypeOuter,
			LinkMeta:       bMetaObj,
			OuterLinkField: bMetaObj.FindField("target"),
		})
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Update(aMetaObj)

		By("and object B is removed")
		metaStore.Remove(bMetaObj.Name)
		By("outer link should be removed from object A")
		// check meta fields
		aMetaObj = metaStore.Get(aMetaDescription.Name)
		Expect(aMetaObj.Fields).To(HaveLen(1))
		Expect(aMetaObj.Fields).To(HaveKey("id"))

	})

	It("does not remove outer field for object if it was not specified in object's description", func() {
		By("having object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		By("and object B, containing generic inner field")
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
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

		By("and object A has been updated with data, which does not have outer generic field")
		aMetaDescription = GetBaseMetaData(aMetaObj.Name)
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Update(aMetaObj)

		aMetaObj = metaStore.Get(aMetaDescription.Name)
		Expect(err).To(BeNil())

		Expect(aMetaObj.Fields).To(HaveLen(2))
	})
})
