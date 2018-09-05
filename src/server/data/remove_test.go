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

var _ = Describe("Records removal", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)
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
	})

	AfterEach(func() {
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
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
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		aRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaObj.Name, map[string]interface{}{}, auth.User{})
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
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		bRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaObj.Name, map[string]interface{}{"a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())
		bKey, _ := bMetaObj.Key.ValueAsString(bRecordData["id"])
		aKey, _ := aMetaObj.Key.ValueAsString(aRecordData["id"])

		//remove A record
		err = dataProcessor.RemoveRecord(globalTransaction.DbTransaction, aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(BeNil())

		//check A record does not exist
		record, _ := dataProcessor.Get(globalTransaction.DbTransaction, aMetaObj.Name, aKey, 1)
		Expect(record).To(BeNil())

		//check B record does not exist
		record, err = dataProcessor.Get(globalTransaction.DbTransaction, bMetaObj.Name, bKey, 1)
		Expect(record).To(BeNil())
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
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		aRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaObj.Name, map[string]interface{}{}, auth.User{})
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
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		bRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaObj.Name, map[string]interface{}{"a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())
		bKey, _ := bMetaObj.Key.ValueAsString(bRecordData["id"])
		aKey, _ := aMetaObj.Key.ValueAsString(aRecordData["id"])

		//remove A record
		err = dataProcessor.RemoveRecord(globalTransaction.DbTransaction, aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(BeNil())

		//check A record does not exist
		record, _ := dataProcessor.Get(globalTransaction.DbTransaction, aMetaObj.Name, aKey, 1)
		Expect(record).To(BeNil())

		//check B record does not exist
		record, err = dataProcessor.Get(globalTransaction.DbTransaction, bMetaObj.Name, bKey, 1)
		Expect(record).To(Not(BeNil()))
		Expect(record.Data["a"]).To(BeNil())
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
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		aRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaObj.Name, map[string]interface{}{}, auth.User{})
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
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaObj.Name, map[string]interface{}{"a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())
		aKey, _ := aMetaObj.Key.ValueAsString(aRecordData["id"])

		//remove A record
		err = dataProcessor.RemoveRecord(globalTransaction.DbTransaction, aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(Not(BeNil()))
	})
})
