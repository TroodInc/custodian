package object

import (
	"encoding/json"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/driver"
	"server/object/meta"
	"utils"
)

var _ = Describe("Outer field", func() {
	appConfig := utils.GetConfig()

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := NewStore(driver)

	AfterEach(func() {
		metaStore.Flush()
	})

	havingAMeta := func() *meta.Meta {
		aMetaObj, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
		Expect(err).To(BeNil())
		return metaStore.Create(aMetaObj)
	}

	havingBMeta := func(A *meta.Meta) *meta.Meta {
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.AddField(&meta.Field{
			Name:     "a",
			Type:     meta.FieldTypeObject,
			LinkType: meta.LinkTypeInner,
			LinkMeta: A,
			Optional: false,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		return metaStore.Create(bMetaObj)
	}

	havingAMetaWithManuallySetBSetLink := func(A, B *meta.Meta) *meta.Meta {
		aMetaDescription := GetBaseMetaData(A.Name)
		aMetaDescription.AddField(&meta.Field{
			Name:           "b_set",
			Type:           meta.FieldTypeArray,
			LinkType:       meta.LinkTypeOuter,
			LinkMeta:       B,
			OuterLinkField: B.FindField("a"),
		})
		(&meta.NormalizationService{}).Normalize(aMetaDescription)
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		return metaStore.Update(aMetaObj)
	}

	It("can create object with manually specified outer field, this field can be used both for querying and retrieving", func() {
		By("having two objects: A and B")
		aMetaObj := havingAMeta()

		bMetaObj := havingBMeta(aMetaObj)

		By("object A containing outer field to B")
		aMetaObj = havingAMetaWithManuallySetBSetLink(aMetaObj, bMetaObj)

		// check meta fields
		aMeta := metaStore.Get(aMetaObj.Name)
		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.Fields).To(HaveKey("b_set"))
		Expect(aMeta.Fields["b_set"].LinkMeta.Name).To(Equal(bMetaObj.Name))
		Expect(aMeta.Fields["b_set"].QueryMode).To(BeTrue())
		Expect(aMeta.Fields["b_set"].RetrieveMode).To(BeTrue())
	})

	It("can create object with automatically added outer field, this field can be used for querying only", func() {
		By("having two objects: A and B")
		aMetaObj := havingAMeta()

		bMetaObj := havingBMeta(aMetaObj)

		aMetaObj = metaStore.Get(aMetaObj.Name)
		bSetField := aMetaObj.FindField(bMetaObj.Name + "_set")
		Expect(bSetField).NotTo(BeNil())
		//automatically added fields should be used only for querying
		Expect(bSetField.QueryMode).To(BeTrue())
		Expect(bSetField.RetrieveMode).To(BeFalse())
	})

	It("can be marshaled to JSON omitting QueryMode and RetrieveMode values", func() {
		By("having two objects: A and B")
		aMetaObj := havingAMeta()
		bMetaObj := havingBMeta(aMetaObj)
		havingAMetaWithManuallySetBSetLink(aMetaObj, bMetaObj)
		// A meta contains automatically generated outer link to B
		aMetaObj = metaStore.Get(aMetaObj.Name)
		aMetaObjForExport := aMetaObj.ForExport()
		encodedData, err := json.Marshal(aMetaObjForExport)
		Expect(err).To(BeNil())
		var decodedData map[string]interface{}
		err = json.Unmarshal(encodedData, &decodedData)
		Expect(err).To(BeNil())
		Expect(decodedData["fields"].([]interface{})[1].(map[string]interface{})).NotTo(HaveKey("queryMode"))
		Expect(decodedData["fields"].([]interface{})[1].(map[string]interface{})).NotTo(HaveKey("retrieveMode"))
	})

	It("replaces automatically added outer field with manually added", func() {
		By("having two objects: A and B")
		aMetaObj := havingAMeta()
		bMetaObj := havingBMeta(aMetaObj)

		// A meta contains automatically generated outer link to B
		aMetaObj = metaStore.Get(aMetaObj.Name)
		Expect(aMetaObj.FindField(bMetaObj.Name + "_set")).NotTo(BeNil())

		//A meta updated with outer link to b
		aMetaDescription := GetBaseMetaData(aMetaObj.Name)
		aMetaDescription.AddField(&meta.Field{
			Name:           "custom_b_set",
			Type:           meta.FieldTypeArray,
			LinkType:       meta.LinkTypeOuter,
			LinkMeta:       bMetaObj,
			OuterLinkField: bMetaObj.FindField("a"),
		})
		(&meta.NormalizationService{}).Normalize(aMetaDescription)
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)

		metaStore.Update(aMetaObj)

		// A meta should contain only custom_b_set, b_set should be removed
		aMetaObj = metaStore.Get(aMetaDescription.Name)
		Expect(err).To(BeNil())
		Expect(aMetaObj.FindField("custom_b_set")).NotTo(BeNil())
		Expect(aMetaObj.FindField("custom_b_set").QueryMode).To(BeTrue())
		Expect(aMetaObj.FindField("custom_b_set").RetrieveMode).To(BeTrue())
		Expect(aMetaObj.FindField(bMetaObj.Name + "_set")).To(BeNil())
	})
})
