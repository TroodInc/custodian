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
)

var _ = Describe("Server", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

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

		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionOptions).Setup()
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
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, metaObj)
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
					Def: map[string]interface{}{
						"func": "nextval",
					},
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
					"id":   int(record["id"].(float64) + 1),
				}
				encodedMetaData, _ := json.Marshal(updateData)

				url := fmt.Sprintf("%s/data/single/%s/%d", appConfig.UrlPrefix, objectA.Name, int(record["id"].(float64)))

				var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
				request.Header.Set("Content-Type", "application/json")
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				Context("response should contain original id and updated name", func() {
					Expect(responseBody).To(Equal("{\"data\":{\"id\":1,\"name\":\"SomeOtherName\"},\"status\":\"OK\"}"))
				})

			})
		})
	})

	It("updates record and outputs its data respecting depth", func() {
		Context("having a record of given object", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			objectA := factoryObjectA(globalTransaction)
			aRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectA.Name, map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())

			objectB := factoryObjectB(globalTransaction)
			bRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectB.Name, map[string]interface{}{"name": "B record", "a": aRecord["id"]}, auth.User{})
			Expect(err).To(BeNil())

			globalTransactionManager.CommitTransaction(globalTransaction)

			Context("and PUT request performed by URL with specified record ID with wrong id specified in body", func() {
				updateData := map[string]interface{}{
					"name": "B record new name",
					"id":   bRecord["id"],
				}
				encodedMetaData, _ := json.Marshal(updateData)

				url := fmt.Sprintf("%s/data/single/%s/%d?depth=2", appConfig.UrlPrefix, objectB.Name, int(bRecord["id"].(float64)))

				var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
				request.Header.Set("Content-Type", "application/json")
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				Context("response should contain nested A record", func() {
					Expect(responseBody).To(Equal("{\"data\":{\"a\":{\"id\":1,\"name\":\"A record\"},\"id\":1,\"name\":\"B record new name\"},\"status\":\"OK\"}"))
				})

			})
		})
	})
})
