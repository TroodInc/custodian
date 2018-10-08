package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"utils"
	"server/object/meta"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/object/description"
	"encoding/json"
)

var _ = Describe("Outer field", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	var globalTransaction *transactions.GlobalTransaction

	BeforeEach(func() {
		var err error

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

	})

	AfterEach(func() {
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("can create object with manually specified outer field, this field can be used both for querying and retrieving", func() {
		By("having two objects: A and B")
		aMetaDescription := description.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaDescription := description.MetaDescription{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		By("object A containing outer field to B")
		aMetaDescription = description.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:           "b_set",
					Type:           description.FieldTypeArray,
					LinkType:       description.LinkTypeOuter,
					LinkMeta:       "b",
					OuterLinkField: "a",
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&aMetaDescription)
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(globalTransaction, aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())

		// check meta fields
		fieldName := "b_set"
		aMeta, _, err := metaStore.Get(globalTransaction, aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.Fields[1].Name).To(Equal(fieldName))
		Expect(aMeta.Fields[1].LinkMeta.Name).To(Equal("b"))
		Expect(aMeta.FindField(fieldName).QueryMode).To(BeTrue())
		Expect(aMeta.FindField(fieldName).RetrieveMode).To(BeTrue())
	})

	It("can create object with automatically added outer field, this field can be used for querying only", func() {
		By("having two objects: A and B")
		aMetaDescription := description.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaDescription := description.MetaDescription{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		aMetaObj, _, err = metaStore.Get(globalTransaction, aMetaDescription.Name, false)
		Expect(err).To(BeNil())
		bSetField := aMetaObj.FindField("b_set")
		Expect(bSetField).NotTo(BeNil())
		//automatically added fields should be used only for querying
		Expect(bSetField.QueryMode).To(BeTrue())
		Expect(bSetField.RetrieveMode).To(BeFalse())
	})

	It("can be marshaled to JSON omitting QueryMode and RetrieveMode values", func() {
		By("having two objects: A and B")
		aMetaDescription := description.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaDescription := description.MetaDescription{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		// A meta contains automatically generated outer link to B
		aMetaObj, _, err = metaStore.Get(globalTransaction, aMetaDescription.Name, false)
		aMetaObjForExport := aMetaObj.InstanceForExport()
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
		aMetaDescription := description.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaDescription := description.MetaDescription{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		// A meta contains automatically generated outer link to B
		aMetaObj, _, err = metaStore.Get(globalTransaction, aMetaDescription.Name, false)
		Expect(err).To(BeNil())
		Expect(aMetaObj.FindField("b_set")).NotTo(BeNil())

		//A meta updated with outer link to b
		aMetaDescription = description.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:           "custom_b_set",
					Type:           description.FieldTypeArray,
					LinkType:       description.LinkTypeOuter,
					LinkMeta:       "b",
					OuterLinkField: "a",
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&aMetaDescription)
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(globalTransaction, aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())

		// A meta should contain only custom_b_set, b_set should be removed
		aMetaObj, _, err = metaStore.Get(globalTransaction, aMetaDescription.Name, false)
		Expect(err).To(BeNil())
		Expect(aMetaObj.FindField("custom_b_set")).NotTo(BeNil())
		Expect(aMetaObj.FindField("custom_b_set").QueryMode).To(BeTrue())
		Expect(aMetaObj.FindField("custom_b_set").RetrieveMode).To(BeTrue())
		Expect(aMetaObj.FindField("b_set")).To(BeNil())
	})
})