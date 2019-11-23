package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"server/data"
	"server/auth"
	"utils"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/object/meta"
	"server/object/description"
)

var _ = Describe("RecordSetOperations removal", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("Can remove records with cascade relation", func() {

		aMetaDescription := description.MetaDescription{
			Name: "a",
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
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		//
		bMetaDescription := description.MetaDescription{
			Name: "b",
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
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					OnDelete: description.OnDeleteCascade.ToVerbose(),
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"a": aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())
		bKey, _ := bMetaObj.Key.ValueAsString(bRecord.Data["id"])
		aKey, _ := aMetaObj.Key.ValueAsString(aRecord.Data["id"])

		//remove A record
		removedData, err := dataProcessor.RemoveRecord(aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(BeNil())

		//check A record does not exist
		record, _ := dataProcessor.Get(aMetaObj.Name, aKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check B record does not exist
		record, err = dataProcessor.Get(bMetaObj.Name, bKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check removed data tree
		Expect(removedData).NotTo(BeNil())
		Expect(removedData).To(HaveKey("b_set"))
		Expect(removedData["b_set"]).To(HaveLen(1))
	})

	It("Can remove record and update child records with 'setNull' relation", func() {

		aMetaDescription := description.MetaDescription{
			Name: "a",
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
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		//
		bMetaDescription := description.MetaDescription{
			Name: "b",
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
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: true,
					OnDelete: description.OnDeleteSetNull.ToVerbose(),
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"a": aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())
		bKey, _ := bMetaObj.Key.ValueAsString(bRecord.Data["id"])
		aKey, _ := aMetaObj.Key.ValueAsString(aRecord.Data["id"])

		//remove A record
		removedData, err := dataProcessor.RemoveRecord(aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(BeNil())

		//check A record does not exist
		record, _ := dataProcessor.Get(aMetaObj.Name, aKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check B record exists
		record, err = dataProcessor.Get(bMetaObj.Name, bKey, nil, nil, 1, false)
		Expect(record).To(Not(BeNil()))
		Expect(record.Data["a"]).To(BeNil())

		//check removed data tree
		Expect(removedData).NotTo(BeNil())
		Expect(removedData).To(Not(HaveKey("b_set")))
	})

	It("Cannot remove record with 'restrict' relation", func() {

		aMetaDescription := description.MetaDescription{
			Name: "a",
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
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		//
		bMetaDescription := description.MetaDescription{
			Name: "b",
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
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: true,
					OnDelete: description.OnDeleteRestrict.ToVerbose(),
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"a": aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())
		aKey, _ := aMetaObj.Key.ValueAsString(aRecord.Data["id"])

		//remove A record
		_, err = dataProcessor.RemoveRecord(aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(Not(BeNil()))
	})

	It("Can remove record and update child records with generic relation and 'setNull' strategy", func() {

		aMetaDescription := description.MetaDescription{
			Name: "a",
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
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		//
		bMetaDescription := description.MetaDescription{
			Name: "b",
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
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{"a"},
					Optional:     true,
					OnDelete:     description.OnDeleteSetNull.ToVerbose(),
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		aKey, _ := aMetaObj.Key.ValueAsString(aRecord.Data["id"])
		bRecord, err := dataProcessor.CreateRecord(
			bMetaObj.Name,
			map[string]interface{}{"target_object": map[string]interface{}{"_object": aMetaObj.Name, "id": aKey}},
			auth.User{},
		)
		Expect(err).To(BeNil())
		bKey, _ := bMetaObj.Key.ValueAsString(bRecord.Data["id"])

		//remove A record
		removedData, err := dataProcessor.RemoveRecord(aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(BeNil())

		//check A record does not exist
		record, _ := dataProcessor.Get(aMetaObj.Name, aKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check B record exists, but generic field value has null value
		record, err = dataProcessor.Get(bMetaObj.Name, bKey, nil, nil, 1, false)
		Expect(record).To(Not(BeNil()))
		Expect(record.Data).To(HaveKey("target_object"))
		Expect(record.Data["target_object"]).To(BeNil())

		//check removed data tree
		Expect(removedData).To(Not(BeNil()))
		Expect(removedData).To(Not(HaveKey("b_set")))
	})

	It("Can remove record and update child records with generic relation and 'cascade' strategy", func() {
		aMetaDescription := description.MetaDescription{
			Name: "a",
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
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		//
		bMetaDescription := description.MetaDescription{
			Name: "b",
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
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{"a"},
					Optional:     true,
					OnDelete:     description.OnDeleteCascade.ToVerbose(),
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		aKey, _ := aMetaObj.Key.ValueAsString(aRecord.Data["id"])
		bRecord, err := dataProcessor.CreateRecord(
			bMetaObj.Name,
			map[string]interface{}{"target_object": map[string]interface{}{"_object": aMetaObj.Name, "id": aKey}},
			auth.User{},
		)
		Expect(err).To(BeNil())
		bKey, _ := bMetaObj.Key.ValueAsString(bRecord.Data["id"])

		//remove A record
		removedData, err := dataProcessor.RemoveRecord(aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(BeNil())

		//check A record does not exist
		record, _ := dataProcessor.Get(aMetaObj.Name, aKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check B record does not exist
		record, err = dataProcessor.Get(bMetaObj.Name, bKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check removed data tree
		Expect(removedData).To(Not(BeNil()))
		Expect(removedData).To(HaveKey("b_set"))
		Expect(removedData["b_set"]).To(HaveLen(1))
	})
})
