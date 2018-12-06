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
	"encoding/json"
	"server/object/description"
)

var _ = Describe("Objects field", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	fileMetaDriver := meta.NewFileMetaDriver("./")
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers

	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(fileMetaDriver.Remove, fileMetaDriver.Create)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	BeforeEach(func() {
		var err error

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		err = metaStore.Flush(globalTransaction)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)

	})

	AfterEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		err = metaStore.Flush(globalTransaction)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("can unmarshal meta with 'objects' field", func() {
		data := map[string]interface{}{
			"name": "a",
			"key":  "id",
			"fields": []interface{}{
				map[string]string{
					"name":     "b_list",
					"type":     "objects",
					"linkType": "inner",
				},
			},
			"cas": false,
		}
		//marshal data into string
		var metaDescription description.MetaDescription
		buffer, err := json.Marshal(data)
		Expect(err).To(BeNil())

		//unmarshal string into metaDescription
		err = json.Unmarshal(buffer, &metaDescription)
		Expect(err).To(BeNil())
		Expect(metaDescription.Fields).To(HaveLen(1))
		Expect(metaDescription.Fields[0].Name).To(Equal("b_list"))
		Expect(metaDescription.Fields[0].Type).To(Equal(description.FieldTypeObjects))
		Expect(metaDescription.Fields[0].LinkType).To(Equal(description.LinkTypeInner))
	})

	It("can build meta with 'objects' field and filled 'throughLink'", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

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
					Optional: true,
				},
			},
		}
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
					Optional: true,
				},
			},
		}
		updatedAMetaDescription := description.MetaDescription{
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
					Optional: true,
				},
				{
					Name:     "b",
					Type:     description.FieldTypeObjects,
					LinkMeta: bMetaDescription.Name,
					LinkType: description.LinkTypeInner,
				},
			},
		}

		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		//check field's properties
		updatedAMetaObj, err := metaStore.NewMeta(&updatedAMetaDescription)
		Expect(err).To(BeNil())
		Expect(updatedAMetaObj.Fields).To(HaveLen(2))
		Expect(updatedAMetaObj.Fields[1].LinkMeta.Name).To(Equal("b"))
		Expect(updatedAMetaObj.Fields[1].Type).To(Equal(description.FieldTypeObjects))
		Expect(updatedAMetaObj.Fields[1].LinkType).To(Equal(description.LinkTypeInner))

		//create meta and check through meta was created
		_, err = metaStore.Update(globalTransaction, updatedAMetaObj.Name, updatedAMetaObj, true)
		Expect(err).To(BeNil())

		throughMeta, _, err := metaStore.Get(globalTransaction, updatedAMetaObj.Fields[1].LinkThrough.Name, false)
		Expect(err).To(BeNil())

		Expect(throughMeta.Name).To(Equal("a__b"))
		Expect(throughMeta.Fields).To(HaveLen(3))

		Expect(throughMeta.Fields[1].Name).To(Equal("a"))
		Expect(throughMeta.Fields[1].Type).To(Equal(description.FieldTypeObject))

		Expect(throughMeta.Fields[2].Name).To(Equal("b"))
		Expect(throughMeta.Fields[2].Type).To(Equal(description.FieldTypeObject))

		globalTransactionManager.RollbackTransaction(globalTransaction)
	})
})
