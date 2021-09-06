package object_test

import (
	"custodian/server/object"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"custodian/server/errors"
	"custodian/server/object/description"

	"custodian/utils"
)

var _ = Describe("PG meta test", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache(), db)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	testObjAName := utils.RandomString(8)
	testObjBName := utils.RandomString(8)
	testObjCName := utils.RandomString(8)

	metaDescriptionA := description.MetaDescription{
		Name: testObjAName,
		Key:  "id",
		Cas:  false,
		Fields: []description.Field{
			{
				Name:     "id",
				Type:     description.FieldTypeNumber,
				Optional: true,
				Def: map[string]interface{}{
					"func": "nextval",
				},
			},
			{
				Name:     "name",
				Type:     description.FieldTypeString,
				Optional: true,
			},
		},
	}
	metaDescriptionB := description.MetaDescription{
		Name: testObjBName,
		Key:  "id",
		Cas:  false,
		Fields: []description.Field{
			{
				Name:     "id",
				Type:     description.FieldTypeNumber,
				Optional: true,
				Def: map[string]interface{}{
					"func": "nextval",
				},
			},
		},
	}

	createObjectA := func() {
		err := metaDescriptionSyncer.Create(metaDescriptionA)
		Expect(err).To(BeNil())

	}

	createObjectB := func() {
		err := metaDescriptionSyncer.Create(metaDescriptionB)
		Expect(err).To(BeNil())

	}

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can't get meta object if it does not exists", func() {
		retrievedMetaDescription, _, err := metaDescriptionSyncer.Get(testObjAName)
		Expect(err.(*errors.ServerError).Code).To(Equal("get_meta"))
		Expect(retrievedMetaDescription).To(BeNil())

	})

	It("can get object from meta", func() {
		createObjectA()

		retrievedMetaDescription, _, err := metaDescriptionSyncer.Get(testObjAName)
		Expect(err).To(BeNil())
		Expect(*retrievedMetaDescription).To(Equal(metaDescriptionA))

	})
	It("can remove object from meta", func() {
		createObjectA()

		retrievedMetaDescription, _, err := metaDescriptionSyncer.Get(testObjAName)
		Expect(err).To(BeNil())
		Expect(*retrievedMetaDescription).To(Equal(metaDescriptionA))

		_, err = metaDescriptionSyncer.Remove(testObjAName)
		Expect(err).To(BeNil())

		retrievedMetaDescription, _, err = metaDescriptionSyncer.Get(testObjAName)
		Expect(err.(*errors.ServerError).Code).To(Equal("get_meta"))
		Expect(retrievedMetaDescription).To(BeNil())

	})

	It("can update meta object", func() {
		createObjectA()
		updatedMetaDescription := metaDescriptionA.Clone()
		updatedMetaDescription.Fields = append(metaDescriptionA.Fields, description.Field{
			Name:     "new_field",
			Type:     description.FieldTypeString,
			Optional: true,
		})

		_, err := metaDescriptionSyncer.Update(testObjAName, *updatedMetaDescription)
		Expect(err).To(BeNil())

		retrievedMetaDescription, _, err := metaDescriptionSyncer.Get(testObjAName)
		Expect(err).To(BeNil())
		Expect(retrievedMetaDescription).NotTo(BeNil())
		Expect(retrievedMetaDescription.Fields).To(HaveLen(3))
		Expect(retrievedMetaDescription).To(Equal(updatedMetaDescription))
	})

	It("can get list of meta objects", func() {
		createObjectA()
		createObjectB()

		metaList, _, err := metaDescriptionSyncer.List()
		Expect(err).To(BeNil())
		Expect(metaList).To(HaveLen(2))

	})

	It("can rename meta object", func() {
		createObjectA()
		updatedMetaDescription := metaDescriptionA.Clone()
		updatedMetaDescription.Name = testObjCName

		_, err := metaDescriptionSyncer.Update(testObjAName, *updatedMetaDescription)
		Expect(err).To(BeNil())

		notExistingMetaDescription, _, err := metaDescriptionSyncer.Get(testObjAName)
		Expect(err).NotTo(BeNil())
		Expect(notExistingMetaDescription).To(BeNil())

		retrievedMetaDescription, _, err := metaDescriptionSyncer.Get(testObjCName)
		Expect(err).To(BeNil())
		Expect(retrievedMetaDescription).NotTo(BeNil())

		Expect(retrievedMetaDescription).NotTo(BeNil())
		Expect(retrievedMetaDescription.Fields).To(HaveLen(2))
		Expect(retrievedMetaDescription.Fields).To(Equal(metaDescriptionA.Fields))
	})
})
