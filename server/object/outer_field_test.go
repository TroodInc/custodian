package object

import (
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/pg"
	"custodian/server/transactions"
	"custodian/utils"
	"encoding/json"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Outer field", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers

	dbTransactionManager := pg.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(dbTransactionManager)
	metaDescriptionSyncer := pg.NewPgMetaDescriptionSyncer(globalTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	havingAMeta := func() *meta.Meta {
		aMetaObj, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	havingBMeta := func(A *meta.Meta) *meta.Meta {
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
			Name:     "a",
			Type:     description.FieldTypeObject,
			LinkType: description.LinkTypeInner,
			LinkMeta: A.Name,
			Optional: false,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())
		return bMetaObj
	}

	havingAMetaWithManuallySetBSetLink := func(A, B *meta.Meta) *meta.Meta {
		aMetaDescription := GetBaseMetaData(A.Name)
		aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
			Name:           "b_set",
			Type:           description.FieldTypeArray,
			LinkType:       description.LinkTypeOuter,
			LinkMeta:       B.Name,
			OuterLinkField: "a",
		})
		(&description.NormalizationService{}).Normalize(aMetaDescription)
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	It("can create object with manually specified outer field, this field can be used both for querying and retrieving", func() {
		By("having two objects: A and B")
		aMetaObj := havingAMeta()

		bMetaObj := havingBMeta(aMetaObj)

		By("object A containing outer field to B")
		aMetaObj = havingAMetaWithManuallySetBSetLink(aMetaObj, bMetaObj)

		// check meta fields
		fieldName := "b_set"
		aMeta, _, err := metaStore.Get(aMetaObj.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.Fields[1].Name).To(Equal(fieldName))
		Expect(aMeta.Fields[1].LinkMeta.Name).To(Equal(bMetaObj.Name))
		Expect(aMeta.FindField(fieldName).QueryMode).To(BeTrue())
		Expect(aMeta.FindField(fieldName).RetrieveMode).To(BeTrue())
	})

	It("can create object with automatically added outer field, this field can be used for querying only", func() {
		By("having two objects: A and B")
		aMetaObj := havingAMeta()

		bMetaObj := havingBMeta(aMetaObj)

		aMetaObj, _, err := metaStore.Get(aMetaObj.Name, false)
		Expect(err).To(BeNil())
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
		aMetaObj, _, err := metaStore.Get(aMetaObj.Name, false)
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
		aMetaObj, _, err := metaStore.Get(aMetaObj.Name, false)
		Expect(err).To(BeNil())
		Expect(aMetaObj.FindField(bMetaObj.Name + "_set")).NotTo(BeNil())

		//A meta updated with outer link to b
		aMetaDescription := GetBaseMetaData(aMetaObj.Name)
		aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
			Name:           "custom_b_set",
			Type:           description.FieldTypeArray,
			LinkType:       description.LinkTypeOuter,
			LinkMeta:       bMetaObj.Name,
			OuterLinkField: "a",
		})
		(&description.NormalizationService{}).Normalize(aMetaDescription)
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())

		// A meta should contain only custom_b_set, b_set should be removed
		aMetaObj, _, err = metaStore.Get(aMetaDescription.Name, false)
		Expect(err).To(BeNil())
		Expect(aMetaObj.FindField("custom_b_set")).NotTo(BeNil())
		Expect(aMetaObj.FindField("custom_b_set").QueryMode).To(BeTrue())
		Expect(aMetaObj.FindField("custom_b_set").RetrieveMode).To(BeTrue())
		Expect(aMetaObj.FindField(bMetaObj.Name + "_set")).To(BeNil())
	})
})
