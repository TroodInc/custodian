package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"fmt"
	"server/pg"

	"bytes"
	"encoding/json"
	"utils"
	"server/object/meta"
	"server/transactions/file_transaction"

	pg_transactions "server/pg/transactions"
	"server/transactions"
	"net/http/httptest"
	"server"
	"server/object/description"
	"server/auth"
	"server/data"
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

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionOptions).Setup(false)
		recorder = httptest.NewRecorder()

	})

	AfterEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("can create the object", func() {
		Context("having valid object description", func() {
			metaData := map[string]interface{}{
				"name": "person",
				"key":  "id",
				"cas":  true,
				"fields": []map[string]interface{}{
					{
						"name":     "id",
						"type":     "number",
						"optional": true,
						"default": map[string]interface{}{
							"func": "nextval",
						},
					}, {
						"name":     "name",
						"type":     "string",
						"optional": false,
					}, {
						"name":     "gender",
						"type":     "string",
						"optional": true,
					}, {
						"name":     "cas",
						"type":     "number",
						"optional": false,
					},
				},
				"actions": []map[string]interface{}{
					{
						"protocol": "REST",
						"method":   "create",
						"args":     []string{"http://localhost:2000/create/contact"},
						"includeValues": map[string]string{
							"amount":        "amount",
							"account__plan": "accountPlan",
						},
						"activeIfNotRoot": true,
					},
				},
			}
			Context("and valid HTTP request object", func() {
				encodedMetaData, _ := json.Marshal(metaData)

				var request, _ = http.NewRequest("PUT", fmt.Sprintf("%s/meta", appConfig.UrlPrefix), bytes.NewBuffer(encodedMetaData))
				request.Header.Set("Content-Type", "application/json")

				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()
				Expect(responseBody).To(Equal(`{"data":{"name":"person","key":"id","fields":[{"name":"id","type":"number","optional":true,"default":{"func":"nextval"}},{"name":"name","type":"string","optional":false},{"name":"gender","type":"string","optional":true},{"name":"cas","type":"number","optional":false}],"actions":[{"method":"create","protocol":"REST","args":["http://localhost:2000/create/contact"],"activeIfNotRoot":true,"includeValues":{"account__plan":"accountPlan","amount":"amount"}}],"cas":true},"status":"OK"}`))

				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				defer func() { globalTransactionManager.RollbackTransaction(globalTransaction) }()
				Expect(err).To(BeNil())

				meta, _, err := metaStore.Get(globalTransaction, "person", true)
				Expect(err).To(BeNil())
				Expect(meta.ActionSet.Original[0].IncludeValues["account__plan"]).To(Equal("accountPlan"))
				Expect(meta.ActionSet.Original[0].IncludeValues["amount"]).To(Equal("amount"))
			})
		})
	})

	It("can remove record with given id", func() {
		Context("having two records of given object", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
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
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, metaObj)
			Expect(err).To(BeNil())

			firstRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaDescription.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())
			dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaDescription.Name, map[string]interface{}{}, auth.User{})

			globalTransactionManager.CommitTransaction(globalTransaction)

			Context("and DELETE request performed by URL with specified record ID", func() {

				url := fmt.Sprintf("%s/data/single/%s/%d", appConfig.UrlPrefix, metaObj.Name, int(firstRecord.Data["id"].(float64)))
				var request, _ = http.NewRequest("DELETE", url, bytes.NewBuffer([]byte{}))
				request.Header.Set("Content-Type", "application/json")
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				defer func() { globalTransactionManager.RollbackTransaction(globalTransaction) }()
				Expect(err).To(BeNil())

				Expect(responseBody).To(Equal(`{"data":{"id":1},"status":"OK"}`))

				Context("and the number of records should be equal to 1 and existing record is not deleted one", func() {
					matchedRecords := []map[string]interface{}{}
					callbackFunction := func(obj map[string]interface{}) error {
						matchedRecords = append(matchedRecords, obj)
						return nil
					}
					dataProcessor.GetBulk(globalTransaction.DbTransaction, metaObj.Name, "", 1, false, callbackFunction)
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0]["id"]).To(Not(Equal(firstRecord.Data["id"])))
				})
			})
		})
	})
})
