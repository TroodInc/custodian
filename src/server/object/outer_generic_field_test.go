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
)

var _ = Describe("Outer generic field", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer)

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

	It("can create object with manually specified outer generic field", func() {
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
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		By("and object C, containing generic inner field")
		cMetaDescription := description.MetaDescription{
			Name: "c",
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name, bMetaObj.Name},
					Optional:     false,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, metaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
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
					Name:           "c_set",
					Type:           description.FieldTypeGeneric,
					LinkType:       description.LinkTypeOuter,
					LinkMeta:       "c",
					OuterLinkField: "target",
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&aMetaDescription)
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(globalTransaction, aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())

		// check meta fields
		fieldName := "c_set"
		aMeta, _, err := metaStore.Get(globalTransaction, aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.Fields[1].Name).To(Equal(fieldName))
		Expect(aMeta.Fields[1].LinkMeta.Name).To(Equal("c"))
		Expect(aMeta.FindField(fieldName).QueryMode).To(BeTrue())
		Expect(aMeta.FindField(fieldName).RetrieveMode).To(BeTrue())
	})

	It("Detects non-existing linked meta", func() {
		By("having an object A, referencing non-existing object B")

		cMetaDescription := description.MetaDescription{
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
					Name:           "target",
					Type:           description.FieldTypeGeneric,
					LinkType:       description.LinkTypeOuter,
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

		By("and object B, containing generic inner field")
		cMetaDescription := description.MetaDescription{
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name},
					Optional:     false,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, metaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
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
					Name:     "b_set",
					Type:     description.FieldTypeGeneric,
					LinkType: description.LinkTypeOuter,
					LinkMeta: "b",
				},
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(Not(BeNil()))

	})

	It("can remove outer generic field from object", func() {
		By("having object A")
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

		By("and object B, containing generic inner field")
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name},
					Optional:     false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
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
					Type:           description.FieldTypeGeneric,
					LinkType:       description.LinkTypeOuter,
					LinkMeta:       "b",
					OuterLinkField: "target",
				},
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(globalTransaction, bMetaObj.Name, bMetaObj, true)
		Expect(err).To(BeNil())

		By("and outer generic field removed from object A")
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
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(globalTransaction, bMetaObj.Name, bMetaObj, true)
		Expect(err).To(BeNil())
	})

	It("removes outer generic field if corresponding inner generic field is removed", func() {
		By("having object A")
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

		By("and object B, containing generic inner field")
		cMetaDescription := description.MetaDescription{
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name},
					Optional:     false,
				},
			},
		}
		cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, cMetaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
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
					Type:           description.FieldTypeGeneric,
					LinkType:       description.LinkTypeOuter,
					LinkMeta:       "b",
					OuterLinkField: "target",
				},
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(globalTransaction, aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())

		//

		By("and inner generic field removed from object B")
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
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(globalTransaction, bMetaObj.Name, bMetaObj, true)
		Expect(err).To(BeNil())

		By("outer link should be removed from object A")
		// check meta fields
		aMetaObj, _, err = metaStore.Get(globalTransaction, aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMetaObj.Fields).To(HaveLen(1))
		Expect(aMetaObj.Fields[0].Name).To(Equal("id"))

	})

	It("removes outer field if object containing corresponding inner field is removed", func() {
		By("having object A")
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

		By("and object B, containing generic inner field")
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name},
					Optional:     false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
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
					Type:           description.FieldTypeGeneric,
					LinkType:       description.LinkTypeOuter,
					LinkMeta:       "b",
					OuterLinkField: "target",
				},
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(globalTransaction, aMetaObj.Name, aMetaObj, true, )
		Expect(err).To(BeNil())

		//

		By("and object B is removed")

		_, err = metaStore.Remove(globalTransaction, bMetaObj.Name, true)
		Expect(err).To(BeNil())

		By("outer link should be removed from object A")
		// check meta fields
		aMetaObj, _, err = metaStore.Get(globalTransaction, aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMetaObj.Fields).To(HaveLen(1))
		Expect(aMetaObj.Fields[0].Name).To(Equal("id"))

	})

	It("does not remove outer field for object if it was not specified in object's description", func() {
		By("having object A")
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

		By("and object B, containing generic inner field")
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name},
					Optional:     false,
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		By("and object A has been updated with data, which does not have outer generic field")
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
			},
		}
		aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(globalTransaction, aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())
		//

		aMetaObj, _, err = metaStore.Get(globalTransaction, aMetaDescription.Name, true)
		Expect(err).To(BeNil())

		Expect(aMetaObj.Fields).To(HaveLen(2))

	})
})
