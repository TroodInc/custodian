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
	"utils"
	"server/transactions/file_transaction"

	"server/object/meta"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/object/description"
	"server"
	"encoding/json"
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

	It("returns all records including total count", func() {

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		objectA := factoryObjectA(globalTransaction)
		for i := 0; i < 50; i++ {
			_, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectA.Name, map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		globalTransactionManager.CommitTransaction(globalTransaction)

		url := fmt.Sprintf("%s/data/bulk/%s?depth=1", appConfig.UrlPrefix, objectA.Name)

		var request, _ = http.NewRequest("GET", url, nil)
		httpServer.Handler.ServeHTTP(recorder, request)
		responseBody := recorder.Body.String()

		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)
		Expect(body["data"].([]interface{})).To(HaveLen(50))
		Expect(body["total_count"].(float64)).To(Equal(float64(50)))
	})

	It("returns slice of records including total count", func() {

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		objectA := factoryObjectA(globalTransaction)
		for i := 0; i < 50; i++ {
			_, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectA.Name, map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())
		}

		globalTransactionManager.CommitTransaction(globalTransaction)

		url := fmt.Sprintf("%s/data/bulk/%s?depth=1&q=limit(0,10)", appConfig.UrlPrefix, objectA.Name)

		var request, _ = http.NewRequest("GET", url, nil)
		httpServer.Handler.ServeHTTP(recorder, request)
		responseBody := recorder.Body.String()

		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)
		Expect(body["data"].([]interface{})).To(HaveLen(10))
		Expect(body["total_count"].(float64)).To(Equal(float64(50)))
	})

	It("returns empty list including total count", func() {

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		objectA := factoryObjectA(globalTransaction)

		globalTransactionManager.CommitTransaction(globalTransaction)

		url := fmt.Sprintf("%s/data/bulk/%s?depth=1", appConfig.UrlPrefix, objectA.Name)

		var request, _ = http.NewRequest("GET", url, nil)
		httpServer.Handler.ServeHTTP(recorder, request)
		responseBody := recorder.Body.String()

		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)
		Expect(body["data"].([]interface{})).To(HaveLen(0))
		Expect(body["total_count"].(float64)).To(Equal(float64(0)))
	})

	It("returns records by query including total count", func() {

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		objectA := factoryObjectA(globalTransaction)

		for i := 0; i < 20; i++ {
			_, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectA.Name, map[string]interface{}{"name": "A"}, auth.User{})
			Expect(err).To(BeNil())
		}
		for i := 0; i < 20; i++ {
			_, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, objectA.Name, map[string]interface{}{"name": "B"}, auth.User{})
			Expect(err).To(BeNil())
		}

		globalTransactionManager.CommitTransaction(globalTransaction)

		url := fmt.Sprintf("%s/data/bulk/%s?depth=1&q=eq(name,B),limit(0,5)", appConfig.UrlPrefix, objectA.Name)

		var request, _ = http.NewRequest("GET", url, nil)
		httpServer.Handler.ServeHTTP(recorder, request)
		responseBody := recorder.Body.String()

		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)
		Expect(body["data"].([]interface{})).To(HaveLen(5))
		Expect(body["total_count"].(float64)).To(Equal(float64(20)))
	})
})
