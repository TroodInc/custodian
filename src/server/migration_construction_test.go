package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"server/data/notifications"
	"server/noti"
	"server/object/meta"
	"server/pg"
	"utils"

	"bytes"
	"encoding/json"
	"fmt"
	"server"
	meta_description "server/migrations/description"

	"server/pg/migrations/managers"
	pg_transactions "server/pg/transactions"
	"server/transactions"
)

var _ = XDescribe("Migration`s construction", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewMetaStore(metaDescriptionSyncer, syncer, globalTransactionManager)

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

	factoryObjectA := func() *meta.Meta {
		metaDescription := meta.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*meta.Field{
				{
					Name:     "id",
					Type:     meta.FieldTypeString,
					Optional: false,
				},
				{
					Name:     "name",
					Type:     meta.FieldTypeString,
					Optional: false,
				},
			},
			Actions: []*notifications.Action{
				{
					Name:     "some-action",
					Method:   notifications.MethodUpdate,
					Protocol: noti.REST,
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
		factoryObjectA()

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
					"method":   notifications.MethodUpdate,
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
	})

	Describe("Objects` operations", func() {
		It("Can create migration to create object", func() {
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
						"method":   notifications.MethodUpdate,
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
		})

		It("Can create migration to rename object", func() {

			factoryObjectA()

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
						"method":   notifications.MethodUpdate,
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
		})

		It("Can create migration to delete object", func() {
			factoryObjectA()

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
		})
	})

	Describe("Fields` operations", func() {
		It("Can create migration to add a new field", func() {
			factoryObjectA()

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
						"method":   notifications.MethodUpdate,
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
		})

		It("Can create migration to remove a field", func() {
			factoryObjectA()

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
						"method":   notifications.MethodUpdate,
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
		})

		It("Can create migration to update a field", func() {
			factoryObjectA()

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
						"method":   notifications.MethodUpdate,
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
		})
	})

	Describe("Actions` operations", func() {
		It("Can create migration to add a new action", func() {
			factoryObjectA()

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
						"method":   notifications.MethodUpdate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-endpoint/"},
					},
					{
						"name":     "some-new-action",
						"method":   notifications.MethodCreate,
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
		})

		It("Can create migration to add a new action", func() {
			factoryObjectA()

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
						"method":   notifications.MethodUpdate,
						"protocol": "REST",
						"args":     []string{"http://localhost:5555/some-endpoint/"},
					},
					{
						"name":     "some-new-action",
						"method":   notifications.MethodCreate,
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
		})

		It("Can create migration to remove an action", func() {
			factoryObjectA()

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
		})

		It("Can create migration to update an action", func() {
			factoryObjectA()

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
						"method":   notifications.MethodUpdate,
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
		})
	})

	It("Creates migration with correct 'dependsOn' values", func() {
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
		err := json.Unmarshal([]byte(responseBody), &body)
		Expect(err).To(BeNil())
		recorder.Body.Reset()

		//check response status
		Expect(body["status"]).To(Equal("OK"))

		migrationDescriptionData := body["data"].(map[string]interface{})
		migrationId := migrationDescriptionData["id"]

		//apply newly generated migration
		encodedMetaData, _ = json.Marshal(migrationDescriptionData)

		url = fmt.Sprintf("%s/migrations/apply", appConfig.UrlPrefix)

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
	})
})
