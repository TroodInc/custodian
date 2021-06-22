package server_test

import (
	"custodian/server/object"
	"fmt"
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"bytes"
	"custodian/utils"
	"encoding/json"

	"custodian/server"
	"custodian/server/auth"
	"custodian/server/object/description"

	"net/http/httptest"
)

var _ = Describe("Server", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)

	
	dataProcessor, _ := object.NewProcessor(metaStore, dbTransactionManager)

	BeforeEach(func() {
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionUrl).Setup(appConfig)
		recorder = httptest.NewRecorder()
	})

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
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
						"name":     "someAction",
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

				var request, _ = http.NewRequest("POST", fmt.Sprintf("%s/meta", appConfig.UrlPrefix), bytes.NewBuffer(encodedMetaData))
				request.Header.Set("Content-Type", "application/json")

				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				err := json.Unmarshal([]byte(responseBody), &body)
				Expect(err).To(BeNil())

				Expect(body["status"].(string)).To(Equal("OK"))
				Expect(body["data"].(map[string]interface{})["name"]).To(Equal("person"))

				meta, _, err := metaStore.Get("person", true)
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
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			firstRecord, err := dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{}, auth.User{})
			Expect(err).To(BeNil())
			dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{}, auth.User{})

			Context("and DELETE request performed by URL with specified record ID", func() {

				url := fmt.Sprintf("%s/data/%s/%d", appConfig.UrlPrefix, metaObj.Name, int(firstRecord.Data["id"].(float64)))
				var request, _ = http.NewRequest("DELETE", url, bytes.NewBuffer([]byte{}))
				request.Header.Set("Content-Type", "application/json")
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				Expect(responseBody).To(Equal(`{"data":{"id":1},"status":"OK"}`))

				Context("and the number of records should be equal to 1 and existing record is not deleted one", func() {
					_, matchedRecords, _ := dataProcessor.GetBulk(metaObj.Name, "", nil, nil, 1, false)
					Expect(matchedRecords).To(HaveLen(1))
					Expect(matchedRecords[0].Data["id"]).To(Not(Equal(firstRecord.Data["id"])))
				})
			})
		})
	})
})
