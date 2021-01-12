package server_test

import (
	"custodian/server/pg"
	"custodian/utils"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"bytes"
	"custodian/server"
	meta_description "custodian/server/migrations/description"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/pg/migrations/managers"
	"custodian/server/transactions"
	"encoding/json"
	"fmt"
)

var _ = Describe("Migration`s construction", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers

	dbTransactionManager := pg.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(dbTransactionManager)
	metaDescriptionSyncer := pg.NewPgMetaDescriptionSyncer(globalTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	BeforeEach(func() {
		//setup server
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionUrl).Setup(appConfig)
		recorder = httptest.NewRecorder()
	})

	flushDb := func() {
		// drop history
		err := managers.NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
		).DropHistory()
		Expect(err).To(BeNil())
		//Flush meta/database
		err = metaStore.Flush()
		Expect(err).To(BeNil())
	}

	factoryObjectA := func(globalTransaction *transactions.GlobalTransaction) *meta.Meta {
		metaDescription := description.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeString,
					Optional: false,
				},
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: false,
				},
			},
			Actions: []description.Action{
				{
					Name:     "some-action",
					Method:   description.MethodUpdate,
					Protocol: description.REST,
					Args:     []string{"http://localhost:5555/some-endpoint/"},
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	BeforeEach(flushDb)
	AfterEach(flushDb)

	It("Does not create migration if changes were not detected", func() {
		Skip("Skiped unless TB-421 will be done.")

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		factoryObjectA(globalTransaction)

		migrationMetaDescription := map[string]interface{}{
			"name":         "a",
			"previousName": "a",
			"key":          "id",
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
			},
			"actions": []map[string]interface{}{
				{
					"name":     "some-action",
					"method":   description.MethodUpdate,
					"protocol": "REST",
					"args":     []string{"http://localhost:5555/some-endpoint/"},
				},
			},
		}

		encodedMetaData, _ := json.Marshal(migrationMetaDescription)

		url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("FAIL"))
		Expect(body["error"].(map[string]interface{})["Code"].(string)).To(Equal("no_changes_were_detected"))

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	Describe("Objects` operations", func() {
		It("Can create migration to create object", func() {
			Skip("Skiped unless TB-421 will be done.")

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			migrationMetaDescription := map[string]interface{}{
				"name":         "a",
				"previousName": "",
				"key":          "id",
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
				},
				"actions": []map[string]interface{}{
					{
						"name":     "some-action",
						"method":   description.MethodUpdate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-endpoint/"},
					},
				},
			}

			encodedMetaData, _ := json.Marshal(migrationMetaDescription)

			url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))

			migrationDescriptionData := body["data"].(map[string]interface{})
			Expect(migrationDescriptionData["applyTo"]).To(Equal(""))

			Expect(migrationDescriptionData["dependsOn"].([]interface{})).To(HaveLen(0))
			Expect(migrationDescriptionData["operations"].([]interface{})).To(HaveLen(1))
			Expect(migrationDescriptionData["operations"].([]interface{})[0].(map[string]interface{})["type"]).To(Equal(meta_description.CreateObjectOperation))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("Can create migration to rename object", func() {
			Skip("Skiped unless TB-421 will be done.")

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			factoryObjectA(globalTransaction)

			migrationMetaDescription := map[string]interface{}{
				"name":         "b",
				"previousName": "a",
				"key":          "id",
				"fields": []map[string]interface{}{
					{
						"name":         "id",
						"type":         "string",
						"optional":     false,
						"previousName": "id",
					},
					{
						"name":         "name",
						"type":         "string",
						"optional":     false,
						"previousName": "name",
					},
				},
				"actions": []map[string]interface{}{
					{
						"name":     "some-action",
						"method":   description.MethodUpdate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-endpoint/"},
					},
				},
			}

			encodedMetaData, _ := json.Marshal(migrationMetaDescription)

			url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))

			migrationDescriptionData := body["data"].(map[string]interface{})
			Expect(migrationDescriptionData["applyTo"]).To(Equal("a"))

			Expect(migrationDescriptionData["dependsOn"].([]interface{})).To(HaveLen(0))
			Expect(migrationDescriptionData["operations"].([]interface{})).To(HaveLen(1))
			Expect(migrationDescriptionData["operations"].([]interface{})[0].(map[string]interface{})["type"]).To(Equal(meta_description.RenameObjectOperation))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("Can create migration to delete object", func() {
			Skip("Skiped unless TB-421 will be done.")

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			factoryObjectA(globalTransaction)

			migrationMetaDescription := map[string]interface{}{
				"name":         "",
				"previousName": "a",
			}

			encodedMetaData, _ := json.Marshal(migrationMetaDescription)

			url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))

			migrationDescriptionData := body["data"].(map[string]interface{})

			Expect(migrationDescriptionData["applyTo"]).To(Equal("a"))
			Expect(migrationDescriptionData["dependsOn"].([]interface{})).To(HaveLen(0))
			Expect(migrationDescriptionData["operations"].([]interface{})).To(HaveLen(1))
			Expect(migrationDescriptionData["operations"].([]interface{})[0].(map[string]interface{})["type"]).To(Equal(meta_description.DeleteObjectOperation))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})
	})

	Describe("Fields` operations", func() {
		It("Can create migration to add a new field", func() {
			Skip("Skiped unless TB-421 will be done.")

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			factoryObjectA(globalTransaction)

			migrationMetaDescription := map[string]interface{}{
				"name":         "a",
				"previousName": "a",
				"key":          "id",
				"fields": []map[string]interface{}{
					{
						"name":         "id",
						"type":         "string",
						"optional":     false,
						"previousName": "id",
					},
					{
						"name":         "name",
						"type":         "string",
						"optional":     false,
						"previousName": "name",
					},
					{
						"name":     "newField",
						"type":     "number",
						"optional": true,
					},
				},
				"actions": []map[string]interface{}{
					{
						"name":     "some-action",
						"method":   description.MethodUpdate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-endpoint/"},
					},
				},
			}

			encodedMetaData, _ := json.Marshal(migrationMetaDescription)

			url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))

			migrationDescriptionData := body["data"].(map[string]interface{})

			Expect(migrationDescriptionData["applyTo"]).To(Equal("a"))
			Expect(migrationDescriptionData["dependsOn"].([]interface{})).To(HaveLen(0))
			Expect(migrationDescriptionData["operations"].([]interface{})).To(HaveLen(1))
			Expect(migrationDescriptionData["operations"].([]interface{})[0].(map[string]interface{})["type"]).To(Equal(meta_description.AddFieldOperation))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("Can create migration to remove a field", func() {
			Skip("Skiped unless TB-421 will be done.")

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			factoryObjectA(globalTransaction)

			migrationMetaDescription := map[string]interface{}{
				"name":         "a",
				"previousName": "a",
				"key":          "id",
				"fields": []map[string]interface{}{
					{
						"name":         "id",
						"type":         "string",
						"optional":     false,
						"previousName": "id",
					},
				},
				"actions": []map[string]interface{}{
					{
						"name":     "some-action",
						"method":   description.MethodUpdate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-endpoint/"},
					},
				},
			}

			encodedMetaData, _ := json.Marshal(migrationMetaDescription)

			url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))

			migrationDescriptionData := body["data"].(map[string]interface{})

			Expect(migrationDescriptionData["applyTo"]).To(Equal("a"))
			Expect(migrationDescriptionData["dependsOn"].([]interface{})).To(HaveLen(0))
			Expect(migrationDescriptionData["operations"].([]interface{})).To(HaveLen(1))
			Expect(migrationDescriptionData["operations"].([]interface{})[0].(map[string]interface{})["type"]).To(Equal(meta_description.RemoveFieldOperation))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("Can create migration to update a field", func() {
			Skip("Skiped unless TB-421 will be done.")

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			factoryObjectA(globalTransaction)

			migrationMetaDescription := map[string]interface{}{
				"name":         "a",
				"previousName": "a",
				"key":          "id",
				"fields": []map[string]interface{}{
					{
						"name":         "id",
						"type":         "string",
						"optional":     false,
						"previousName": "id",
					},
					{
						"name":         "renamed_field",
						"type":         "string",
						"optional":     true,
						"previousName": "name",
					},
				},
				"actions": []map[string]interface{}{
					{
						"name":     "some-action",
						"method":   description.MethodUpdate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-endpoint/"},
					},
				},
			}

			encodedMetaData, _ := json.Marshal(migrationMetaDescription)

			url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))

			migrationDescriptionData := body["data"].(map[string]interface{})

			Expect(migrationDescriptionData["applyTo"]).To(Equal("a"))
			Expect(migrationDescriptionData["dependsOn"].([]interface{})).To(HaveLen(0))
			Expect(migrationDescriptionData["operations"].([]interface{})).To(HaveLen(1))
			Expect(migrationDescriptionData["operations"].([]interface{})[0].(map[string]interface{})["type"]).To(Equal(meta_description.UpdateFieldOperation))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})
	})

	Describe("Actions` operations", func() {
		It("Can create migration to add a new action", func() {
			Skip("Skiped unless TB-421 will be done.")

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			factoryObjectA(globalTransaction)

			migrationMetaDescription := map[string]interface{}{
				"name":         "a",
				"previousName": "a",
				"key":          "id",
				"fields": []map[string]interface{}{
					{
						"name":         "id",
						"type":         "string",
						"optional":     false,
						"previousName": "id",
					},
					{
						"name":         "name",
						"type":         "string",
						"optional":     false,
						"previousName": "name",
					},
				},
				"actions": []map[string]interface{}{
					{
						"name":     "some-action",
						"method":   description.MethodUpdate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-endpoint/"},
					},
					{
						"name":     "some-new-action",
						"method":   description.MethodCreate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-endpoint/"},
					},
				},
			}

			encodedMetaData, _ := json.Marshal(migrationMetaDescription)

			url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))

			migrationDescriptionData := body["data"].(map[string]interface{})

			Expect(migrationDescriptionData["applyTo"]).To(Equal("a"))
			Expect(migrationDescriptionData["dependsOn"].([]interface{})).To(HaveLen(0))
			Expect(migrationDescriptionData["operations"].([]interface{})).To(HaveLen(1))
			Expect(migrationDescriptionData["operations"].([]interface{})[0].(map[string]interface{})["type"]).To(Equal(meta_description.AddActionOperation))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("Can create migration to add a new action", func() {
			Skip("Skiped unless TB-421 will be done.")

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			factoryObjectA(globalTransaction)

			migrationMetaDescription := map[string]interface{}{
				"name":         "a",
				"previousName": "a",
				"key":          "id",
				"fields": []map[string]interface{}{
					{
						"name":         "id",
						"type":         "string",
						"optional":     false,
						"previousName": "id",
					},
					{
						"name":         "name",
						"type":         "string",
						"optional":     false,
						"previousName": "name",
					},
				},
				"actions": []map[string]interface{}{
					{
						"name":     "some-action",
						"method":   description.MethodUpdate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-endpoint/"},
					},
					{
						"name":     "some-new-action",
						"method":   description.MethodCreate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-endpoint/"},
					},
				},
			}

			encodedMetaData, _ := json.Marshal(migrationMetaDescription)

			url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))

			migrationDescriptionData := body["data"].(map[string]interface{})

			Expect(migrationDescriptionData["applyTo"]).To(Equal("a"))
			Expect(migrationDescriptionData["dependsOn"].([]interface{})).To(HaveLen(0))
			Expect(migrationDescriptionData["operations"].([]interface{})).To(HaveLen(1))
			Expect(migrationDescriptionData["operations"].([]interface{})[0].(map[string]interface{})["type"]).To(Equal(meta_description.AddActionOperation))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("Can create migration to remove an action", func() {
			Skip("Skiped unless TB-421 will be done.")

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			factoryObjectA(globalTransaction)

			migrationMetaDescription := map[string]interface{}{
				"name":         "a",
				"previousName": "a",
				"key":          "id",
				"fields": []map[string]interface{}{
					{
						"name":         "id",
						"type":         "string",
						"optional":     false,
						"previousName": "id",
					},
					{
						"name":         "name",
						"type":         "string",
						"optional":     false,
						"previousName": "name",
					},
				},
				"actions": []string{},
			}

			encodedMetaData, _ := json.Marshal(migrationMetaDescription)

			url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))

			migrationDescriptionData := body["data"].(map[string]interface{})

			Expect(migrationDescriptionData["applyTo"]).To(Equal("a"))
			Expect(migrationDescriptionData["dependsOn"].([]interface{})).To(HaveLen(0))
			Expect(migrationDescriptionData["operations"].([]interface{})).To(HaveLen(1))
			Expect(migrationDescriptionData["operations"].([]interface{})[0].(map[string]interface{})["type"]).To(Equal(meta_description.RemoveActionOperation))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("Can create migration to update an action", func() {
			Skip("Skiped unless TB-421 will be done.")

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			factoryObjectA(globalTransaction)

			migrationMetaDescription := map[string]interface{}{
				"name":         "a",
				"previousName": "a",
				"key":          "id",
				"fields": []map[string]interface{}{
					{
						"name":         "id",
						"type":         "string",
						"optional":     false,
						"previousName": "id",
					},
					{
						"name":         "name",
						"type":         "string",
						"optional":     false,
						"previousName": "name",
					},
				},
				"actions": []map[string]interface{}{
					{
						"name":     "some-action",
						"method":   description.MethodUpdate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-updated-endpoint/"},
					},
				},
			}

			encodedMetaData, _ := json.Marshal(migrationMetaDescription)

			url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))

			migrationDescriptionData := body["data"].(map[string]interface{})

			Expect(migrationDescriptionData["applyTo"]).To(Equal("a"))
			Expect(migrationDescriptionData["dependsOn"].([]interface{})).To(HaveLen(0))
			Expect(migrationDescriptionData["operations"].([]interface{})).To(HaveLen(1))
			Expect(migrationDescriptionData["operations"].([]interface{})[0].(map[string]interface{})["type"]).To(Equal(meta_description.UpdateActionOperation))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})
	})

	It("Creates migration with correct 'dependsOn' values", func() {
		Skip("Skiped unless TB-421 will be done.")

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		//Step 1: create and apply a migration to create the object
		migrationMetaDescription := map[string]interface{}{
			"name":         "a",
			"previousName": "",
			"key":          "id",
			"fields": []map[string]interface{}{
				{
					"name":     "id",
					"type":     "string",
					"optional": true,
				},
				{
					"name":     "name",
					"type":     "string",
					"optional": true,
				},
			},
		}

		encodedMetaData, _ := json.Marshal(migrationMetaDescription)

		url := fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		err = json.Unmarshal([]byte(responseBody), &body)
		Expect(err).To(BeNil())
		recorder.Body.Reset()

		//check response status
		Expect(body["status"]).To(Equal("OK"))

		migrationDescriptionData := body["data"].(map[string]interface{})
		migrationId := migrationDescriptionData["id"]

		//apply newly generated migration
		encodedMetaData, _ = json.Marshal(migrationDescriptionData)

		url = fmt.Sprintf("%s/migrations/", appConfig.UrlPrefix)

		request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		body = map[string]interface{}{}
		err = json.Unmarshal([]byte(recorder.Body.String()), &body)
		Expect(err).To(BeNil())
		recorder.Body.Reset()

		//check response status
		Expect(body["status"]).To(Equal("OK"))

		//Step 2: construct a new migration to update the object

		migrationMetaDescription = map[string]interface{}{
			"name":         "b",
			"previousName": "a",
			"key":          "id",
			"fields": []map[string]interface{}{
				{
					"name":     "id",
					"type":     "string",
					"optional": true,
				},
				{
					"name":     "name",
					"type":     "string",
					"optional": true,
				},
			},
		}

		encodedMetaData, _ = json.Marshal(migrationMetaDescription)

		url = fmt.Sprintf("%s/migrations/construct", appConfig.UrlPrefix)

		request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody = recorder.Body.String()
		body = map[string]interface{}{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))

		migrationDescriptionData = body["data"].(map[string]interface{})
		Expect(migrationDescriptionData["dependsOn"].([]interface{})).To(HaveLen(1))
		Expect(migrationDescriptionData["dependsOn"].([]interface{})[0].(string)).To(Equal(migrationId))

		globalTransactionManager.CommitTransaction(globalTransaction)
	})
})
