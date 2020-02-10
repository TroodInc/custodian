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
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can create object with manually specified outer generic field", func() {
		By("having two objects: A and B")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("and object C, containing generic inner field")
		cMetaDescription := GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.Fields = append(cMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{aMetaObj.Name, bMetaObj.Name},
			Optional:     false,
		})
		metaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
		aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
			Name:           "c_set",
			Type:           description.FieldTypeGeneric,
			LinkType:       description.LinkTypeOuter,
			LinkMeta:       metaObj.Name,
			OuterLinkField: "target",
		})

		(&description.NormalizationService{}).Normalize(aMetaDescription)
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())

		// check meta fields
		fieldName := "c_set"
		aMeta, _, err := metaStore.Get(aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.Fields[1].Name).To(Equal(fieldName))
		Expect(aMeta.Fields[1].LinkMeta.Name).To(Equal(metaObj.Name))
		Expect(aMeta.FindField(fieldName).QueryMode).To(BeTrue())
		Expect(aMeta.FindField(fieldName).RetrieveMode).To(BeTrue())
	})

	It("Detects non-existing linked meta", func() {
		By("having an object A, referencing non-existing object B")

		cMetaDescription := GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.Fields = append(cMetaDescription.Fields, description.Field{
			Name:           "target",
			Type:           description.FieldTypeGeneric,
			LinkType:       description.LinkTypeOuter,
			LinkMeta:       "b",
			OuterLinkField: "some_field",
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
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("and object B, containing generic inner field")
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{aMetaObj.Name},
			Optional:     false,
		})
		metaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
		aMetaDescription = GetBaseMetaData(utils.RandomString(8))
		aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
			Name:     "b_set",
			Type:     description.FieldTypeGeneric,
			LinkType: description.LinkTypeOuter,
			LinkMeta: metaObj.Name,
		})
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(Not(BeNil()))

	})

	It("can remove outer generic field from object", func() {
		By("having object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("and object B, containing generic inner field")
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{aMetaObj.Name},
			Optional:     false,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
		aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
			Name:           "b_set",
			Type:           description.FieldTypeGeneric,
			LinkType:       description.LinkTypeOuter,
			LinkMeta:       bMetaObj.Name,
			OuterLinkField: "target",
		})
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(bMetaObj.Name, bMetaObj, true)
		Expect(err).To(BeNil())

		By("and outer generic field removed from object A")
		aMetaDescription = GetBaseMetaData(aMetaObj.Name)
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(bMetaObj.Name, bMetaObj, true)
		Expect(err).To(BeNil())
	})

	It("removes outer generic field if corresponding inner generic field is removed", func() {
		By("having object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("and object B, containing generic inner field")
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{aMetaObj.Name},
			Optional:     false,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
		aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
			Name:           "b_set",
			Type:           description.FieldTypeGeneric,
			LinkType:       description.LinkTypeOuter,
			LinkMeta:       bMetaObj.Name,
			OuterLinkField: "target",
		})
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())

		//

		By("and inner generic field removed from object B")
		bMetaDescription = GetBaseMetaData(bMetaObj.Name)
		bMetaObj, err = metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(bMetaObj.Name, bMetaObj, true)
		Expect(err).To(BeNil())

		By("outer link should be removed from object A")
		// check meta fields
		aMetaObj, _, err = metaStore.Get(aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMetaObj.Fields).To(HaveLen(1))
		Expect(aMetaObj.Fields[0].Name).To(Equal("id"))

	})

	It("removes outer field if object containing corresponding inner field is removed", func() {
		By("having object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("and object B, containing generic inner field")
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{aMetaObj.Name},
			Optional:     false,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("and outer generic field added to object A")
		aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
			Name:           "b_set",
			Type:           description.FieldTypeGeneric,
			LinkType:       description.LinkTypeOuter,
			LinkMeta:       bMetaObj.Name,
			OuterLinkField: "target",
		})
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true, )
		Expect(err).To(BeNil())

		//

		By("and object B is removed")
		_, err = metaStore.Remove(bMetaObj.Name, true)
		Expect(err).To(BeNil())
		By("outer link should be removed from object A")
		// check meta fields
		aMetaObj, _, err = metaStore.Get(aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMetaObj.Fields).To(HaveLen(1))
		Expect(aMetaObj.Fields[0].Name).To(Equal("id"))

	})

	It("does not remove outer field for object if it was not specified in object's description", func() {
		By("having object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		aMetaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("and object B, containing generic inner field")
		bMetaDescription := GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{aMetaObj.Name},
			Optional:     false,
		})
		bMetaObj, err := metaStore.NewMeta(bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("and object A has been updated with data, which does not have outer generic field")
		aMetaDescription = GetBaseMetaData(aMetaObj.Name)
		aMetaObj, err = metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())
		//

		aMetaObj, _, err = metaStore.Get(aMetaDescription.Name, true)
		Expect(err).To(BeNil())

		Expect(aMetaObj.Fields).To(HaveLen(2))

	})
})
