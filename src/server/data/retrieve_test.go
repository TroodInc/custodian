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

var _ = Describe("Data", func() {
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

	It("can outputs by 'Objects' field values respecting specified depth value set to 1", func() {
		Context("having an object with outer link to another object", func() {
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
					{
						Name: "name",
						Type: description.FieldTypeString,
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
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "as",
						Type:     description.FieldTypeObjects,
						LinkType: description.LinkTypeInner,
						LinkMeta: "a",
						Optional: true,
					},
				},
			}
			bMetaObject, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(globalTransaction, bMetaObject)

			aRecordName := "Some-A-record"
			anotherARecordName := "Another-some-A-record"
			//create B record with A record
			bRecord, err := dataProcessor.CreateRecord(
				globalTransaction.DbTransaction,
				bMetaObject.Name,
				map[string]interface{}{
					"as": []interface{}{
						map[string]interface{}{
							"name": aRecordName,
						},
						map[string]interface{}{
							"name": anotherARecordName,
						},
					},
				},
				auth.User{},
			)
			Expect(err).To(BeNil())

			//create B record which should not be in query result
			_, err = dataProcessor.CreateRecord(
				globalTransaction.DbTransaction,
				bMetaObject.Name,
				map[string]interface{}{},
				auth.User{},
			)
			Expect(err).To(BeNil())

			//
			bRecord, err = dataProcessor.Get(globalTransaction.DbTransaction, bMetaObject.Name, bRecord.PkAsString(), 1, false)
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("as"))
			Expect(bRecord.Data).To(HaveLen(2))
			Expect(bRecord.Data["as"].([]interface{})[0].(float64)).To(Equal(1.0))
			Expect(bRecord.Data["as"].([]interface{})[1].(float64)).To(Equal(2.0))
		})
	})

	It("can outputs by 'Objects' field values respecting specified depth value set to 2", func() {
		Context("having an object with outer link to another object", func() {
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
					{
						Name: "name",
						Type: description.FieldTypeString,
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
						Name:     "id",
						Type:     description.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "as",
						Type:     description.FieldTypeObjects,
						LinkType: description.LinkTypeInner,
						LinkMeta: "a",
						Optional: true,
					},
				},
			}
			bMetaObject, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(globalTransaction, bMetaObject)

			aRecordName := "Some-A-record"
			anotherARecordName := "Another-some-A-record"
			//create B record with A record
			bRecord, err := dataProcessor.CreateRecord(
				globalTransaction.DbTransaction,
				bMetaObject.Name,
				map[string]interface{}{
					"as": []interface{}{
						map[string]interface{}{
							"name": aRecordName,
						},
						map[string]interface{}{
							"name": anotherARecordName,
						},
					},
				},
				auth.User{},
			)
			Expect(err).To(BeNil())

			//create B record which should not be in query result
			_, err = dataProcessor.CreateRecord(
				globalTransaction.DbTransaction,
				bMetaObject.Name,
				map[string]interface{}{},
				auth.User{},
			)
			Expect(err).To(BeNil())

			//
			bRecord, err = dataProcessor.Get(globalTransaction.DbTransaction, bMetaObject.Name, bRecord.PkAsString(), 2, false)
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("as"))
			Expect(bRecord.Data["as"]).To(HaveLen(2))
			Expect(bRecord.Data["as"].([]interface{})).To(HaveLen(2))
			Expect(bRecord.Data["as"].([]interface{})[0].(map[string]interface{})["id"]).To(Equal(1.0))
			Expect(bRecord.Data["as"].([]interface{})[1].(map[string]interface{})["id"]).To(Equal(2.0))
		})
	})
})
