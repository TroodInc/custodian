package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"custodian/server/errors"
	"custodian/server/object/meta"
	"custodian/server/pg"
	"custodian/server/transactions"
	"custodian/server/transactions/file_transaction"
	"custodian/utils"
	"custodian/server/object/description"

)

var _ = Describe("PG meta test", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg.NewPgMetaDescriptionSyncer(dbTransactionManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	testObjAName := utils.RandomString(8)
	testObjBName := utils.RandomString(8)

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
})
