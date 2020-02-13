package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	. "server/object/driver"
	"server/object/meta"
	"utils"
)

var _ = FDescribe("Test Refactored Meta Store", func() {

	Context("With Json Driver", func() {
		appConfig := utils.GetConfig()
		tmpMetaDir, _ := ioutil.TempDir("", "meta_")

		driver := NewJsonDriver(appConfig.DbConnectionUrl, tmpMetaDir)
		store  := NewStore(driver)

		AfterEach(func() {
			store.Flush()
		})

		RunSharedCases(driver, store)
	})

	XContext("With Postgres Driver", func() {
		appConfig := utils.GetConfig()

		driver := NewPostgresDriver(appConfig.DbConnectionUrl)
		store  := NewStore(driver)

		AfterEach(func() {
			store.Flush()
		})

		RunSharedCases(driver, store)
	})

})


func RunSharedCases(driver MetaDriver, store *Store) {
	It("Must get meta", func() {
		name := utils.RandomString(8)
		driver.Create(GetBaseMetaData(name))

		meta := store.Get(name)
		Expect(meta).NotTo(BeNil())
		Expect(meta.Name).To(Equal(name))
	})

	It("Must list meta", func() {
		name := utils.RandomString(8)
		driver.Create(GetBaseMetaData(name))

		metaList := store.List()

		Expect(metaList).To(HaveLen(1))
		Expect(metaList[0].Name).To(Equal(name))
	})

	It("Creates meta", func() {
		createdMeta := store.Create(
			GetBaseMetaData(utils.RandomString(8)),
		)

		objectToTest := driver.Get(createdMeta.Name)

		Expect(objectToTest).NotTo(BeNil())
		Expect(objectToTest).To(Equal(createdMeta))
	})

	It("Creates reverse outer links for new meta", func() {
		aMetaObj, bMetaObj := GetTwoBaseLinkedObjects(store)

		objectToTest := store.Get(aMetaObj.Name)

		reverseField := objectToTest.FindField(bMetaObj.Name + "_set")
		Expect(reverseField).NotTo(BeNil())
		Expect(reverseField.Type).To(Equal(meta.FieldTypeArray))
		Expect(reverseField.LinkType).To(Equal(meta.LinkTypeOuter))
		Expect(reverseField.LinkMeta.Name).To(Equal(bMetaObj.Name))
		Expect(reverseField.OuterLinkField.Name).To(Equal("a"))
	})

	It("Creates reverse outer links for generic metas", func() {
		aMetaObj, bMetaObj := GetTwoBaseLinkedObjects(store)
		cMetaObject := GetBaseMetaData(utils.RandomString(8))
		cMetaObject.AddField(&meta.Field{
			Name:           "target",
			Type:           meta.FieldTypeGeneric,
			LinkType:       meta.LinkTypeInner,
			LinkMetaList:   []*meta.Meta{aMetaObj, bMetaObj},
		})
		store.Create(cMetaObject)

		objectAToTest := store.Get(aMetaObj.Name)
		Expect(objectAToTest.Fields).To(HaveKey(cMetaObject.Name + "_set"))

		objectBToTest := store.Get(bMetaObj.Name)
		Expect(objectBToTest.Fields).To(HaveKey(cMetaObject.Name + "_set"))
	})

	It("Saving manually created outer links", func() {
		aMetaObj, bMetaObj := GetTwoBaseLinkedObjects(store)
		aMetaObj.AddField(&meta.Field{
			Name:           "custom_b_set",
			Type:           meta.FieldTypeArray,
			LinkType:       meta.LinkTypeOuter,
			LinkMeta:       bMetaObj,
			OuterLinkField: bMetaObj.FindField("a"),
		})
		store.Update(aMetaObj)

		objectToTest := store.Get(aMetaObj.Name)
		Expect(objectToTest.Fields).To(HaveKey("custom_b_set"))
	})

	It("Removes reverse outer links on meta remove", func() {
		aMetaObj, bMetaObj := GetTwoBaseLinkedObjects(store)
		store.Remove(bMetaObj.Name)

		objectToTest := store.Get(aMetaObj.Name)

		reverseField := objectToTest.FindField(bMetaObj.Name + "_set")
		Expect(reverseField).To(BeNil())
	})

	It("Removes inner links on meta remove", func() {
		aMetaObj, bMetaObj := GetTwoBaseLinkedObjects(store)
		store.Remove(aMetaObj.Name)

		objectToTest := store.Get(bMetaObj.Name)

		reverseField := objectToTest.FindField("a")
		Expect(reverseField).To(BeNil())
	})
}