package data_test

import (
	"custodian/server/auth"
	"custodian/server/data"
	"custodian/server/data/record"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/pg"
	"custodian/server/transactions"
	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := pg.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(dbTransactionManager)
	metaDescriptionSyncer := pg.NewPgMetaDescriptionSyncer(globalTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can outputs by 'Objects' field values respecting specified depth value set to 1", func() {
		Context("having an object with outer link to another object", func() {
			testObjAName := utils.RandomString(8)
			testObjBName := utils.RandomString(8)
			aMetaDescription := description.MetaDescription{
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
						Name: "name",
						Type: description.FieldTypeString,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())

			bMetaDescription := description.MetaDescription{
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
					{
						Name:     "as",
						Type:     description.FieldTypeObjects,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjAName,
						Optional: true,
					},
				},
			}
			bMetaObject, err := metaStore.NewMeta(&bMetaDescription)
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
			testObjAName := utils.RandomString(8)
			testObjBName := utils.RandomString(8)
			aMetaDescription := description.MetaDescription{
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
						Name: "name",
						Type: description.FieldTypeString,
					},
				},
			}
			aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMetaObj)
			Expect(err).To(BeNil())

			bMetaDescription := description.MetaDescription{
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
					{
						Name:     "as",
						Type:     description.FieldTypeObjects,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjAName,
						Optional: true,
					},
				},
			}
			bMetaObject, err := metaStore.NewMeta(&bMetaDescription)
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
