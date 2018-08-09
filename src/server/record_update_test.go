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
	metaStore := object.NewStore(object.NewFileMetaDriver("./"), syncer)

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

	factoryObjectA := func() *object.Meta {
		metaDescription := object.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name:     "id",
					Type:     object.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "name",
					Type:     object.FieldTypeString,
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
		return metaObj
	}

	factoryObjectB := func() *object.Meta {
		metaDescription := object.MetaDescription{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name:     "id",
					Type:     object.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "name",
					Type:     object.FieldTypeString,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "a",
					Type:     object.FieldTypeObject,
					LinkType: object.LinkTypeInner,
					LinkMeta: "a",
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	It("updates record with the given id, omitting id specified in body", func() {
		Context("having a record of given object", func() {
			objectA := factoryObjectA()
			record, err := dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{"name": "SomeName"}, auth.User{}, true)
			Expect(err).To(BeNil())
			//create another record to ensure only specified record is affected by update
			_, err = dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{"name": "SomeName"}, auth.User{}, true)
			Expect(err).To(BeNil())

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
			objectA := factoryObjectA()
			aRecord, err := dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{"name": "A record"}, auth.User{}, true)
			Expect(err).To(BeNil())

			objectB := factoryObjectB()
			bRecord, err := dataProcessor.CreateRecord(objectB.Name, map[string]interface{}{"name": "B record", "a": aRecord["id"]}, auth.User{}, true)
			Expect(err).To(BeNil())

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
