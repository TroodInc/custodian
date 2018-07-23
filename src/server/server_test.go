package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server"
	"net/http"
	"fmt"
	"net/http/httptest"
	"server/pg"
	"server/meta"
	"server/data"
	"server/auth"
	"bytes"
	"encoding/json"
	"utils"
)

var _ = Describe("Server", func() {
	appConfig := utils.GetConfig()

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)

	BeforeEach(func() {
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionOptions).Setup()
		recorder = httptest.NewRecorder()
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
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
				Expect(responseBody).To(Equal("{\"status\":\"OK\"}"))

				meta, _, err := metaStore.Get("person", true)
				Expect(err).To(BeNil())
				Expect(meta.Actions.Original[0].IncludeValues["account__plan"]).To(Equal("accountPlan"))
				Expect(meta.Actions.Original[0].IncludeValues["amount"]).To(Equal("amount"))
			})
		})
	})

	It("can remove record with given id", func() {
		Context("having two records of given object", func() {
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			firstRecord, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())
			dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{}, auth.User{})

			Context("and DELETE request performed by URL with specified record ID", func() {

				url := fmt.Sprintf("%s/data/single/%s/%d", appConfig.UrlPrefix, metaObj.Name, int(firstRecord["id"].(float64)))
				var request, _ = http.NewRequest("DELETE", url, bytes.NewBuffer([]byte{}))
				request.Header.Set("Content-Type", "application/json")
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				Context("response should be OK", func() {
					Expect(responseBody).To(Equal("{\"status\":\"OK\"}"))
				})

				Context("and the number of records should be equal to 1 and existing record is not deleted one", func() {
					matchedRecords := []map[string]interface{}{}
					callbackFunction := func(obj map[string]interface{}) error {
						matchedRecords = append(matchedRecords, obj)
						return nil
					}
					dataProcessor.GetBulk(metaObj.Name, "", 1, callbackFunction)
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0]["id"]).To(Not(Equal(firstRecord["id"])))
				})

			})
		})
	})

	It("updates record with the given id, omitting id specified in body", func() {
		Context("having a record of given object", func() {
			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "id",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			record, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "SomeName"}, auth.User{})
			Expect(err).To(BeNil())
			//create another record to ensure only specified record is affected by update
			_, err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "SomeName"}, auth.User{})
			Expect(err).To(BeNil())

			Context("and PUT request performed by URL with specified record ID with wrong id specified in body", func() {
				updateData := map[string]interface{}{
					"name": "SomeOtherName",
					"id":   int(record["id"].(float64) + 1),
				}
				encodedMetaData, _ := json.Marshal(updateData)

				url := fmt.Sprintf("%s/data/single/%s/%d", appConfig.UrlPrefix, metaObj.Name, int(record["id"].(float64)))

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
})
