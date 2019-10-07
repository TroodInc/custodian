package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"fmt"
	"net/http/httptest"
	"server/pg"
	"server/data"
	"server/auth"
	"bytes"
	"encoding/json"
	"utils"
	"server/transactions/file_transaction"

	"server/object/meta"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/object/description"
	"server"
	"server/data/record"
)

var _ = Describe("Server", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer)

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

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
		globalTransactionManager.CommitTransaction(globalTransaction)

		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionOptions).Setup(appConfig)
		recorder = httptest.NewRecorder()

	})

	AfterEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	factoryObjectA := func(globalTransaction *transactions.GlobalTransaction) *meta.Meta {
		metaDescription := description.MetaDescription{
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
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	factoryObjectAWithManuallySetOuterLinkToB := func(globalTransaction *transactions.GlobalTransaction) *meta.Meta {
		metaDescription := description.MetaDescription{
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
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
				{
					Name:           "b_set",
					Type:           description.FieldTypeArray,
					LinkType:       description.LinkTypeOuter,
					LinkMeta:       "b",
					OuterLinkField: "a",
					Optional:       true,
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&metaDescription)
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(globalTransaction, metaObj.Name, metaObj, true)
		Expect(err).To(BeNil())
		return metaObj
	}

	factoryObjectB := func(globalTransaction *transactions.GlobalTransaction) *meta.Meta {
		metaDescription := description.MetaDescription{
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
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
				{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	factoryObjectCWithObjectsLinkToA := func(globalTransaction *transactions.GlobalTransaction) *meta.Meta {
		cMetaDescription := description.MetaDescription{
			Name: "c",
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
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
				{
					Name:     "as",
					Type:     description.FieldTypeObjects,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&cMetaDescription)
		cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, cMetaObj)
		Expect(err).To(BeNil())
		return cMetaObj
	}

	It("Cant update M2M field by adding objects", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		aMetaObject := factoryObjectA(globalTransaction)
		cMetaObject := factoryObjectCWithObjectsLinkToA(globalTransaction)

		aRecordFirst, err := dataProcessor.CreateRecord(
			globalTransaction.DbTransaction, aMetaObject.Name,
			map[string]interface{}{"name": "first obj m2m test"}, auth.User{},
		)
		Expect(err).To(BeNil())

		aRecordSecond, err := dataProcessor.CreateRecord(
			globalTransaction.DbTransaction, aMetaObject.Name,
			map[string]interface{}{"name": "second obj m2m test"}, auth.User{},
		)
		Expect(err).To(BeNil())

		aRecordThird, err := dataProcessor.CreateRecord(
			globalTransaction.DbTransaction, aMetaObject.Name,
			map[string]interface{}{"name": "third obj m2m test"}, auth.User{},
		)
		Expect(err).To(BeNil())


		cRecord, err := dataProcessor.CreateRecord(
			globalTransaction.DbTransaction, cMetaObject.Name,
			map[string]interface{}{
				"name": "root obj m2m test", "as":[]interface{}{aRecordFirst.Pk(), aRecordSecond.Pk()},
			},
			auth.User{},
		)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)

		url := fmt.Sprintf("%s/data/%s/%d", appConfig.UrlPrefix, cMetaObject.Name, int(cRecord.Data["id"].(float64)))

		encodedMetaData, _ := json.Marshal(map[string]interface{}{
			"as":  []interface{}{aRecordFirst.Pk(), aRecordSecond.Pk(), aRecordThird.Pk()},
		})

		var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		Expect(recorder.Code).To(Equal(200))

		var body map[string]interface{}
		json.Unmarshal([]byte(recorder.Body.String()), &body)
		Expect(body["data"].(map[string]interface{})["as"]).To(Equal([]interface{}{
			aRecordFirst.Pk(), aRecordSecond.Pk(), aRecordThird.Pk(),
		}))
	})

	It("updates record with the given id, omitting id specified in body", func() {
		Context("having a record of given object", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			objectA := factoryObjectA(globalTransaction)
			record, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectA.Name, map[string]interface{}{"name": "SomeName"}, auth.User{})
			Expect(err).To(BeNil())
			//create another record to ensure only specified record is affected by update
			_, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectA.Name, map[string]interface{}{"name": "SomeName"}, auth.User{})
			Expect(err).To(BeNil())

			globalTransactionManager.CommitTransaction(globalTransaction)

			Context("and PUT request performed by URL with specified record ID with wrong id specified in body", func() {
				updateData := map[string]interface{}{
					"name": "SomeOtherName",
					"id":   int(record.Data["id"].(float64) + 1),
				}
				encodedMetaData, _ := json.Marshal(updateData)

				url := fmt.Sprintf("%s/data/%s/%d", appConfig.UrlPrefix, objectA.Name, int(record.Data["id"].(float64)))

				var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
				request.Header.Set("Content-Type", "application/json")
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				Context("response should contain original id and updated name", func() {
					Expect(responseBody).To(Equal("{\"data\":{\"id\":1,\"name\":\"SomeOtherName\"},\"status\":\"OK\"}"))
				})

			})
		})
	})

	Context("having a record of given object", func() {
		var bRecord *record.Record
		var objectB *meta.Meta

		BeforeEach(func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			objectA := factoryObjectA(globalTransaction)
			aRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectA.Name, map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())

			objectB = factoryObjectB(globalTransaction)
			bRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectB.Name, map[string]interface{}{"name": "B record", "a": aRecord.Data["id"]}, auth.User{})
			Expect(err).To(BeNil())

			factoryObjectAWithManuallySetOuterLinkToB(globalTransaction)

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("updates record and outputs its data respecting depth", func() {
			updateData := map[string]interface{}{
				"name": "B record new name",
				"id":   bRecord.Data["id"],
			}
			encodedMetaData, _ := json.Marshal(updateData)

			url := fmt.Sprintf("%s/data/%s/%d?depth=2", appConfig.UrlPrefix, objectB.Name, int(bRecord.Data["id"].(float64)))

			var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			Expect(responseBody).To(Equal("{\"data\":{\"a\":{\"b_set\":[1],\"id\":1,\"name\":\"A record\"},\"id\":1,\"name\":\"B record new name\"},\"status\":\"OK\"}"))
		})

		It("updates record and outputs its data respecting depth, omitting field specified in 'exclude' key", func() {
			updateData := map[string]interface{}{
				"name": "B record new name",
				"id":   bRecord.Data["id"],
			}
			encodedMetaData, _ := json.Marshal(updateData)

			url := fmt.Sprintf("%s/data/%s/%d?depth=2,exclude=a", appConfig.UrlPrefix, objectB.Name, int(bRecord.Data["id"].(float64)))

			var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			Expect(responseBody).To(Equal("{\"data\":{\"a\":1,\"id\":1,\"name\":\"B record new name\"},\"status\":\"OK\"}"))
		})
	})
})
