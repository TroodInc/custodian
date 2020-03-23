package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"server/object"
	"server/object/meta"
	"server/pg"
	"utils"

	"bytes"
	"encoding/json"
	"fmt"
	"server"

	"server/pg/migrations/managers"
	pg_transactions "server/pg/transactions"
	"server/transactions"
)

var _ = Describe("Server", func() {
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
		objName := utils.RandomString(8)
		migrationDescriptionData := map[string]interface{}{
			"id":        utils.RandomString(8),
			"applyTo":   "",
			"dependsOn": []string{},
			"operations": []map[string]interface{}{
				{
					"type": "createObject",
					"object": map[string]interface{}{
						"name": objName,
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

		url := fmt.Sprintf("%s/migrations/apply", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been created
		aMeta, _, err := metaStore.Get(objName, false)
		Expect(err).To(BeNil())
		Expect(aMeta).NotTo(BeNil())
	})

	It("Can fake migration appliance", func() {
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

		url := fmt.Sprintf("%s/migrations/apply?fake=true", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been created
		aMeta, _, err := metaStore.Get("a", false)
		Expect(err).NotTo(BeNil())
		Expect(aMeta).To(BeNil())
		appliedMigrations, err := migrationManager.GetPrecedingMigrationsForObject("a")
		Expect(err).To(BeNil())
		Expect(appliedMigrations).To(HaveLen(1))
		Expect(appliedMigrations[0].Data["migration_id"]).To(Equal(migrationDescriptionData["id"]))
	})

	It("Can rename object by application of migration", func() {
		//Create A object
		metaObj := object.GetBaseMetaData(utils.RandomString(8))
		err := metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		//apply migration

		migrationDescriptionData := map[string]interface{}{
			"id":        utils.RandomString(8),
			"applyTo":   aMetaDescription.Name,
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
								"type": meta.FieldTypeNumber,
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

		url := fmt.Sprintf("%s/migrations/apply", appConfig.UrlPrefix)

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
		Expect(err).To(BeNil())
		Expect(dMeta).NotTo(BeNil())
	})

	It("Can delete object by application of migration", func() {
		//Create A object
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))

		metaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		//apply migration

		migrationDescriptionData := map[string]interface{}{
			"id":        utils.RandomString(8),
			"applyTo":   aMetaDescription.Name,
			"dependsOn": []string{},
			"operations": []map[string]interface{}{
				{
					"type": "deleteObject",
					"object": map[string]interface{}{
						"name": aMetaDescription.Name,
						"key":  "id",
						"cas":  false,
						"fields": []map[string]interface{}{
							{
								"name": "id",
								"type": meta.FieldTypeNumber,
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

		url := fmt.Sprintf("%s/migrations/apply", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been renamed

		dMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
		Expect(dMeta).To(BeNil())
		Expect(err).NotTo(BeNil())
	})

	It("Can add field by application of migration", func() {
		//Create A object
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))

		metaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		//apply migration

		migrationDescriptionData := map[string]interface{}{
			"id":        utils.RandomString(8),
			"applyTo":   aMetaDescription.Name,
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

		url := fmt.Sprintf("%s/migrations/apply", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been renamed

		aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
		Expect(err).To(BeNil())

		Expect(aMeta.Fields).To(HaveLen(2))
	})

	It("Can rename field by application of migration", func() {
		//Create A object
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		aMetaDescription.Fields = append(aMetaDescription.Fields, &meta.Field{
			Name: "some_field",
			Type: meta.FieldTypeString,
			Def:  "def-string",
		})

		metaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		//apply migration

		migrationDescriptionData := map[string]interface{}{
			"id":        utils.RandomString(8),
			"applyTo":   aMetaDescription.Name,
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

		url := fmt.Sprintf("%s/migrations/apply", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been renamed

		aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
		Expect(err).To(BeNil())

		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.FindField("some_new_field")).NotTo(BeNil())
	})

	It("Can remove field by appliance of migration", func() {
		//Create A object
		aMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		aMetaDescription.Fields = append(aMetaDescription.Fields, &meta.Field{
			Name: "some_field",
			Type: meta.FieldTypeString,
			Def:  "def-string",
		})

		metaObj, err := metaStore.NewMeta(aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		//apply migration

		migrationDescriptionData := map[string]interface{}{
			"id":        utils.RandomString(8),
			"applyTo":   aMetaDescription.Name,
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

		url := fmt.Sprintf("%s/migrations/apply", appConfig.UrlPrefix)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure meta has been renamed

		aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
		Expect(err).To(BeNil())

		Expect(aMeta.Fields).To(HaveLen(1))
	})

	It("Does`nt apply migration with invalid parent ID", func() {
		migrationDescriptionData := map[string]interface{}{
			"id":        "b5df723r",
			"applyTo":   "",
			"dependsOn": []string{"some-non-existing-id"},
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

		url := fmt.Sprintf("%s/migrations/apply", appConfig.UrlPrefix)

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
