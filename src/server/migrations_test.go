package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"server/pg"
	"utils"
	"server/transactions/file_transaction"

	"server/object/meta"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server"
	"encoding/json"
	"bytes"
	"fmt"
	"server/object/description"
	"server/pg/migrations/operations/object"
	"server/pg/migrations/managers"
)

var _ = Describe("Server", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer)

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	BeforeEach(func() {
		//setup server
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionOptions).Setup(false)
		recorder = httptest.NewRecorder()
	})

	flushDb := func() {
		//Flush meta/database
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		err = metaStore.Flush(globalTransaction)
		Expect(err).To(BeNil())
		// drop history
		err = managers.NewMigrationManager(dataManager, metaDescriptionSyncer).DropHistory(globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	}

	BeforeEach(flushDb)
	AfterEach(flushDb)

	It("Can create object by application of migration", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		migrationDescriptionData := map[string]interface{}{
			"id":        "b5df723r",
			"applyTo":   "",
			"dependsOn": []string{},
			"operations": []map[string]interface{}{
				{
					"type": "createObject",
					"object": map[string]interface{}{
						"name": "a",
						"key":  "id",
						"fields": []map[string]interface{}{
							{
								"name":     "id",
								"type":     "string",
								"optional": false,
							},
							{
								"name":     "name",
								"type":     "string",
								"optional": false,
							},
							{
								"name":     "order",
								"type":     "number",
								"optional": true,
							},
						},
						"cas": false,
					},
				},
			},
		}

		encodedMetaData, _ := json.Marshal(migrationDescriptionData)

		url := fmt.Sprintf("%s/migration/apply", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been created
		aMeta, _, err := metaStore.Get(globalTransaction, "a", false)
		Expect(err).To(BeNil())
		Expect(aMeta).NotTo(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("Can rename object by application of migration", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		//Create A object
		aMetaDescription := &description.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}

		aMetaOjb, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(aMetaDescription)
		Expect(err).To(BeNil())

		createOperation := object.NewCreateObjectOperation(aMetaOjb)
		aMetaOjb, err = createOperation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = createOperation.SyncDbDescription(nil, globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		err = globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
		//apply migration

		migrationDescriptionData := map[string]interface{}{
			"id":        "jsdf7823",
			"applyTo":   "a",
			"dependsOn": []string{},
			"operations": []map[string]interface{}{
				{
					"type": "renameObject",
					"object": map[string]interface{}{
						"name": "d",
						"key":  "id",
						"cas":  false,
						"fields": []map[string]interface{}{
							{
								"name": "id",
								"type": description.FieldTypeNumber,
								"default": map[string]interface{}{
									"func": "nextval",
								},
							},
						},
					},
				},
			},
		}

		encodedMetaData, _ := json.Marshal(migrationDescriptionData)

		url := fmt.Sprintf("%s/migration/apply", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been renamed

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		dMeta, _, err := metaStore.Get(globalTransaction, "d", false)
		Expect(err).To(BeNil())
		Expect(dMeta).NotTo(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})
})
