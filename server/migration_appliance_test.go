package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"custodian/server/pg"
	"custodian/utils"
	"custodian/server/transactions/file_transaction"

	"custodian/server/object/meta"
	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions"
	"custodian/server"
	"encoding/json"
	"bytes"
	"fmt"
	"custodian/server/object/description"
	"custodian/server/pg/migrations/operations/object"
	"custodian/server/pg/migrations/managers"
	"custodian/server/pg_meta"

)

var _ = Describe("Server 101", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg_meta.NewPgMetaDescriptionSyncer(dbTransactionManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	migrationManager := managers.NewMigrationManager(
		metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
	)

	BeforeEach(func() {
		//setup server
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionUrl).Setup(appConfig)
		recorder = httptest.NewRecorder()
	})

	flushDb := func() {
		// drop history
		err := migrationManager.DropHistory()
		Expect(err).To(BeNil())
		//Flush meta/database
		err = metaStore.Flush()
		Expect(err).To(BeNil())
	}

	AfterEach(flushDb)

	It("Can create object by application of migration", func() {
		testObjAName := utils.RandomString(8)
		migrationDescriptionData := map[string]interface{}{
			"id":        "b5df723r",
			"applyTo":   "",
			"dependsOn": []string{},
			"operations": []map[string]interface{}{
				{
					"type": "createObject",
					"object": map[string]interface{}{
						"name": testObjAName,
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

		url := fmt.Sprintf("%s/migrations", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been created
		aMeta, _, err := metaStore.Get(testObjAName, false)
		Expect(err).To(BeNil())
		Expect(aMeta).NotTo(BeNil())
	})

	It("Can fake migration appliance", func() {
		testObjAName := utils.RandomString(8)
		migrationDescriptionData := map[string]interface{}{
			"id":        "b5df723r",
			"applyTo":   "",
			"dependsOn": []string{},
			"operations": []map[string]interface{}{
				{
					"type": "createObject",
					"object": map[string]interface{}{
						"name": testObjAName,
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

		url := fmt.Sprintf("%s/migrations?fake=true", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been created
		aMeta, _, err := metaStore.Get(testObjAName, false)
		Expect(err).NotTo(BeNil())
		Expect(aMeta).To(BeNil())

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		appliedMigrations, err := migrationManager.GetPrecedingMigrationsForObject(testObjAName)
		Expect(err).To(BeNil())
		Expect(appliedMigrations).To(HaveLen(1))
		Expect(appliedMigrations[0].Data["id"]).To(Equal(migrationDescriptionData["id"]))

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("Can rename object by application of migration", func() {
		Skip("Repair flush db.")
		testObjAName := utils.RandomString(8)
		testObjDName := utils.RandomString(8)
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		//Create A object
		aMetaDescription := &description.MetaDescription{
			Name: testObjAName,
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

		createOperation := object.NewCreateObjectOperation(aMetaDescription)
		aMetaDescription, err = createOperation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = createOperation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		err = globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
		//apply migration

		migrationDescriptionData := map[string]interface{}{
			"id":        "jsdf7823",
			"applyTo":   testObjAName,
			"dependsOn": []string{},
			"operations": []map[string]interface{}{
				{
					"type": "renameObject",
					"object": map[string]interface{}{
						"name": testObjDName,
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

		url := fmt.Sprintf("%s/migrations", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		// //ensure meta has been renamed

		dMeta, _, err := metaStore.Get(testObjDName, false)
		Expect(err).To(BeNil())
		Expect(dMeta).NotTo(BeNil())
	})

	It("Can delete object by application of migration", func() {
		testObjAName := utils.RandomString(8)
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		//Create A object
		aMetaDescription := &description.MetaDescription{
			Name: testObjAName,
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

		createOperation := object.NewCreateObjectOperation(aMetaDescription)
		aMetaDescription, err = createOperation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = createOperation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		err = globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
		//apply migration

		migrationDescriptionData := map[string]interface{}{
			"id":        "jsdf7823",
			"applyTo":   testObjAName,
			"dependsOn": []string{},
			"operations": []map[string]interface{}{
				{
					"type": "deleteObject",
					"object": map[string]interface{}{
						"name": testObjAName,
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

		url := fmt.Sprintf("%s/migrations", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been renamed

		dMeta, _, err := metaStore.Get("d", false)
		Expect(dMeta).To(BeNil())
		Expect(err).NotTo(BeNil())
	})

	It("Can add field by application of migration", func() {
		testObjAName := utils.RandomString(8)
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		//Create A object
		aMetaDescription := &description.MetaDescription{
			Name: testObjAName,
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

		createOperation := object.NewCreateObjectOperation(aMetaDescription)
		aMetaDescription, err = createOperation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = createOperation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		err = globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
		//apply migration

		migrationDescriptionData := map[string]interface{}{
			"id":        "qsGsd7823",
			"applyTo":   testObjAName,
			"dependsOn": []string{},
			"operations": []map[string]interface{}{
				{
					"type": "addField",
					"field": map[string]interface{}{
						"name":    "new-field",
						"type":    "string",
						"default": "some-default",
					},
				},
			},
		}

		encodedMetaData, _ := json.Marshal(migrationDescriptionData)

		url := fmt.Sprintf("%s/migrations", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been renamed

		aMeta, _, err := metaStore.Get(testObjAName, false)
		Expect(err).To(BeNil())

		Expect(aMeta.Fields).To(HaveLen(2))
	})

	It("Can rename field by application of migration", func() {
		testObjAName := utils.RandomString(8)
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		//Create A object
		aMetaDescription := &description.MetaDescription{
			Name: testObjAName,
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
				{
					Name: "some_field",
					Type: description.FieldTypeString,
					Def:  "def-string",
				},
			},
		}

		createOperation := object.NewCreateObjectOperation(aMetaDescription)
		aMetaDescription, err = createOperation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = createOperation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		err = globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
		//apply migration

		migrationDescriptionData := map[string]interface{}{
			"id":        "q3sdfsgd",
			"applyTo":   testObjAName,
			"dependsOn": []string{},
			"operations": []map[string]interface{}{
				{
					"type": "updateField",
					"field": map[string]interface{}{
						"previousName": "some_field",
						"name":         "some_new_field",
						"type":         "string",
						"default":      "some-default",
					},
				},
			},
		}

		encodedMetaData, _ := json.Marshal(migrationDescriptionData)

		url := fmt.Sprintf("%s/migrations", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been renamed

		aMeta, _, err := metaStore.Get(testObjAName, false)
		Expect(err).To(BeNil())

		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.FindField("some_new_field")).NotTo(BeNil())
	})

	It("Can remove field by appliance of migration", func() {
		testObjAName := utils.RandomString(8)
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		//Create A object
		aMetaDescription := &description.MetaDescription{
			Name: testObjAName,
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
				{
					Name: "some_field",
					Type: description.FieldTypeString,
					Def:  "def-string",
				},
			},
		}

		createOperation := object.NewCreateObjectOperation(aMetaDescription)
		aMetaDescription, err = createOperation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = createOperation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		err = globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
		//apply migration

		migrationDescriptionData := map[string]interface{}{
			"id":        "q3sdfsgd7823",
			"applyTo":   testObjAName,
			"dependsOn": []string{},
			"operations": []map[string]interface{}{
				{
					"type": "removeField",
					"field": map[string]interface{}{
						"name":    "some_field",
						"type":    "string",
						"default": "some-default",
					},
				},
			},
		}

		encodedMetaData, _ := json.Marshal(migrationDescriptionData)

		url := fmt.Sprintf("%s/migrations", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been renamed

		aMeta, _, err := metaStore.Get(testObjAName, false)
		Expect(err).To(BeNil())

		Expect(aMeta.Fields).To(HaveLen(1))
	})

	It("Does`nt apply migration with invalid parent ID", func() {
		testObjAName := utils.RandomString(8)
		migrationDescriptionData := map[string]interface{}{
			"id":        "b5df723r",
			"applyTo":   "",
			"dependsOn": []string{"some-non-existing-id"},
			"operations": []map[string]interface{}{
				{
					"type": "createObject",
					"object": map[string]interface{}{
						"name": testObjAName,
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

		url := fmt.Sprintf("%s/migrations", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("FAIL"))
		//ensure meta has been renamed
	})
})
