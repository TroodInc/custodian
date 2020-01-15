package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/data/record"
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
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	BeforeEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	Context("having an object with outer link to another object", func() {
		var bMetaObject *meta.Meta
		var bRecord *record.Record

		BeforeEach(func() {
			aMetaDescription := description.GetBasicMetaDescription("random")
			aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
				Name: "name",
				Type: description.FieldTypeString,
			})
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())

			bMetaDescription := description.GetBasicMetaDescription("random")
			bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
				Name:     "as",
				Type:     description.FieldTypeObjects,
				LinkType: description.LinkTypeInner,
				LinkMeta: aMetaObj.Name,
				Optional: true,
			})

			bMetaObject, err = metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMetaObject)

			aRecordName := "Some-A-record"
			anotherARecordName := "Another-some-A-record"
			//create B record with A record
			bRecord, err = dataProcessor.CreateRecord(
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
				bMetaObject.Name,
				map[string]interface{}{},
				auth.User{},
			)
			Expect(err).To(BeNil())
		})

		It("can outputs by 'Objects' field values respecting specified depth value set to 1", func() {
			bRecord, err := dataProcessor.Get(bMetaObject.Name, bRecord.PkAsString(), nil, nil, 1, false)
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("as"))
			Expect(bRecord.Data).To(HaveLen(2))
			Expect(bRecord.Data["as"].([]interface{})[0].(float64)).To(Equal(1.0))
			Expect(bRecord.Data["as"].([]interface{})[1].(float64)).To(Equal(2.0))
		})


		It("can outputs by 'Objects' field values respecting specified depth value set to 2", func() {
			bRecord, err := dataProcessor.Get(bMetaObject.Name, bRecord.PkAsString(), nil, nil, 1, false)
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("as"))
			Expect(bRecord.Data["as"]).To(HaveLen(2))
			Expect(bRecord.Data["as"].([]interface{})).To(HaveLen(2))
			Expect(bRecord.Data["as"].([]interface{})[0].(*record.Record).Data["id"]).To(Equal(1.0))
			Expect(bRecord.Data["as"].([]interface{})[1].(*record.Record).Data["id"]).To(Equal(2.0))
		})
	})
})
