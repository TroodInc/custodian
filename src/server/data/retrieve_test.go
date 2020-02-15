package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/auth"
	"server/data"
	"server/data/record"
	"server/object"
	"server/object/driver"
	"server/object/meta"

	"server/pg"
	"server/pg/transactions"
	"utils"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	dbTransactionManager := transactions.NewPgDbTransactionManager(dataManager)

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := object.NewStore(driver)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		metaStore.Flush()
	})

	FIt("can outputs by 'Objects' field values respecting specified depth value set to 1", func() {
		Context("having an object with outer link to another object", func() {
			aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			aMetaDescription.AddField(&meta.Field{
				Name: "name",
				Type: meta.FieldTypeString,
			})
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMetaObj)

			bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField(&meta.Field{
				Name:     "as",
				Type:     meta.FieldTypeObjects,
				LinkType: meta.LinkTypeInner,
				LinkMeta: aMetaObj,
				Optional: true,
			})
			bMetaObject, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMetaObject)

			aRecordName := "Some-A-record"
			anotherARecordName := "Another-some-A-record"
			//create B record with A record
			bRecord, err := dataProcessor.CreateRecord(
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

			//
			bRecord, err = dataProcessor.Get(bMetaObject.Name, bRecord.PkAsString(), nil, nil, 1, false)
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("as"))
			Expect(bRecord.Data).To(HaveLen(2))
			Expect(bRecord.Data["as"].([]interface{})[0].(float64)).To(Equal(1.0))
			Expect(bRecord.Data["as"].([]interface{})[1].(float64)).To(Equal(2.0))
		})
	})

	It("can outputs by 'Objects' field values respecting specified depth value set to 2", func() {
		Context("having an object with outer link to another object", func() {
			aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			aMetaDescription.AddField(&meta.Field{Name: "name", Type: meta.FieldTypeString})
			aMetaObj, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMetaObj)

			bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.AddField(&meta.Field{
				Name:     "as",
				Type:     meta.FieldTypeObjects,
				LinkType: meta.LinkTypeInner,
				LinkMeta: aMetaObj,
				Optional: true,
			})
			bMetaObject, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMetaObject)

			aRecordName := "Some-A-record"
			anotherARecordName := "Another-some-A-record"
			//create B record with A record
			bRecord, err := dataProcessor.CreateRecord(
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

			//
			bRecord, err = dataProcessor.Get(bMetaObject.Name, bRecord.PkAsString(), nil, nil, 2, false)
			Expect(err).To(BeNil())
			Expect(bRecord.Data).To(HaveKey("as"))
			Expect(bRecord.Data["as"]).To(HaveLen(2))
			Expect(bRecord.Data["as"].([]interface{})).To(HaveLen(2))
			Expect(bRecord.Data["as"].([]interface{})[0].(*record.Record).Data["id"]).To(Equal(1.0))
			Expect(bRecord.Data["as"].([]interface{})[1].(*record.Record).Data["id"]).To(Equal(2.0))
		})
	})
})
