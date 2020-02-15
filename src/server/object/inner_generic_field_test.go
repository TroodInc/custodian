package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/driver"
	"server/object/meta"
	"utils"
)

var _ = Describe("Inner generic field", func() {
	appConfig := utils.GetConfig()

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := NewStore(driver)

	AfterEach(func() {
		metaStore.Flush()
	})


	It("can create object with inner generic field", func() {
		By("having two objects: A and B")
		aMetaObj, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		bMetaObj, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
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

		metaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(metaObj)

		// check meta fields
		cMeta := metaStore.Get(cMetaDescription.Name)
		Expect(err).To(BeNil())
		Expect(cMeta.Fields).To(HaveLen(2))
		Expect(cMeta.Fields["target"].LinkMetaList).To(HaveLen(2))
	})

	It("Validates linked metas", func() {
		By("having an object A, referencing non-existing object B")
		bMeta := GetBaseMetaData(utils.RandomString(8))

		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaDescription.AddField(&meta.Field{
			Name:         "target",
			Type:         meta.FieldTypeGeneric,
			LinkType:     meta.LinkTypeInner,
			LinkMetaList: []*meta.Meta{bMeta},
			Optional:     false,
		})
		By("MetaDescription should not be created")
		_, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(Not(BeNil()))
	})

	It("can remove generic field from object", func() {

		By("having object A with generic field")
		metaDescription := GetBaseMetaData(utils.RandomString(8))
		metaDescription.AddField(&meta.Field{
			Name:         "target",
			Type:         meta.FieldTypeGeneric,
			LinkType:     meta.LinkTypeInner,
			LinkMetaList: []*meta.Meta{},
			Optional:     false,
		})
		metaObj, err := metaStore.NewMeta(metaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(metaObj)
		By("when generic field is removed from object and object has been updated")

		metaDescription = GetBaseMetaData(utils.RandomString(8))
		metaObj, err = metaStore.NewMeta(metaDescription)
		Expect(err).To(BeNil())
		metaStore.Update(metaObj)

		// check meta fields
		cMeta := metaStore.Get(metaDescription.Name)
		Expect(cMeta.Fields).To(HaveKey("id"))
	})

	It("does not leave orphan links in LinkMetaList on object removal", func() {
		By("having two objects A and B reference by generic field of object C")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(bMetaObj)

		cMetaDescription := GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.AddField(&meta.Field{
			Name:         "target",
			Type:         meta.FieldTypeGeneric,
			LinkType:     meta.LinkTypeInner,
			LinkMetaList: []*meta.Meta{aMetaObj, bMetaObj},
			Optional:     false,
		})
		metaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(metaObj)

		By("since object A is deleted, it should be removed from LinkMetaList")

		metaStore.Remove(aMetaObj.Name)

		cMetaObj := metaStore.Get(cMetaDescription.Name)
		Expect(err).To(BeNil())
		Expect(cMetaObj.Fields["target"].LinkMetaList).To(HaveLen(1))
	})

	It("can create object with inner generic field", func() {
		By("having object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(aMetaObj)

		By("and object C, containing generic inner field")

		cMetaDescription := GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.AddField(&meta.Field{
			Name:         "target",
			Type:         meta.FieldTypeGeneric,
			LinkType:     meta.LinkTypeInner,
			LinkMetaList: []*meta.Meta{aMetaObj},
			Optional:     false,
		})
		metaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		metaStore.Create(metaObj)

		// check meta fields
		aMeta := metaStore.Get(aMetaDescription.Name)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.Fields).To(HaveKey(cMetaDescription.Name + "_set"))
		Expect(aMeta.Fields[cMetaDescription.Name + "_set"].LinkType).To(Equal(meta.LinkTypeOuter))
		Expect(aMeta.Fields[cMetaDescription.Name + "_set"].Type).To(Equal(meta.FieldTypeGeneric))
	})
})
