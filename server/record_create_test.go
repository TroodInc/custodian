package server_test

import (
	"bytes"
	"custodian/server/auth"
	"custodian/server/object"
	"custodian/utils"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"custodian/server"
	"custodian/server/object/description"
)

var _ = Describe("Server", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache(), db)
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

	factoryObjectA := func() *object.Meta {
		metaDescription := description.MetaDescription{
			Name: "a_qbhbj",
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
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	factoryObjectB := func() *object.Meta {
		metaDescription := description.MetaDescription{
			Name: "b_bezv9",
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
				{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a_qbhbj",
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	factoryObjectAWithManuallySetOuterLinkToB := func() *object.Meta {
		metaDescription := description.MetaDescription{
			Name: "a_qbhbj",
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
				{
					Name:           "b_set",
					Type:           description.FieldTypeArray,
					LinkType:       description.LinkTypeOuter,
					LinkMeta:       "b_bezv9",
					OuterLinkField: "a",
					Optional:       true,
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&metaDescription)
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(metaObj.Name, metaObj, true, true)
		Expect(err).To(BeNil())
		return metaObj
	}
	Context("having a record of given object", func() {
		var aRecord *object.Record
		var objectB *object.Meta
		BeforeEach(func() {
			objectA := factoryObjectA()
			objectB = factoryObjectB()
			factoryObjectAWithManuallySetOuterLinkToB()

			aRecord, _ = dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		})

		It("creates record and outputs its data respecting depth", func() {
			createData := map[string]interface{}{
				"name": "B record name",
				"a":    aRecord.Data["id"],
			}
			encodedMetaData, _ := json.Marshal(createData)

			url := fmt.Sprintf("%s/data/%s?depth=2", appConfig.UrlPrefix, objectB.Name)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			Context("response should contain nested A record", func() {
				Expect(responseBody).To(Equal(`{"data":{"a":{"b_set":[1],"id":1,"name":"A record"},"id":1,"name":"B record name"},"status":"OK"}`))
			})
		})

		It("creates record and outputs its data respecting depth, omitting fields specified in 'exclude' key", func() {
			createData := map[string]interface{}{
				"name": "B record name",
				"a":    aRecord.Data["id"],
			}
			encodedMetaData, _ := json.Marshal(createData)

			url := fmt.Sprintf("%s/data/%s?depth=2,exclude=a", appConfig.UrlPrefix, objectB.Name)

			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			Context("response should contain nested A record", func() {
				Expect(responseBody).To(Equal(`{"data":{"a":1,"id":1,"name":"B record name"},"status":"OK"}`))
			})
		})

		It("must set id sequence properly after creating records with forced Id's", func() {
			createData := map[string]interface{}{
				"id":   777,
				"name": "B record name",
				"a":    aRecord.Data["id"],
			}

			encodedMetaData, _ := json.Marshal(createData)

			url := fmt.Sprintf("%s/data/%s?depth=2,exclude=a", appConfig.UrlPrefix, objectB.Name)

			request, _ := http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)
			result := recorder.Body.String()
			Expect(result).To(Equal(`{"data":{"a":1,"id":777,"name":"B record name"},"status":"OK"}`))

			nextData := map[string]interface{}{
				"name": "B record name",
				"a":    aRecord.Data["id"],
			}

			recorderNext := httptest.NewRecorder()

			encodedNextData, _ := json.Marshal(nextData)
			requestNext, _ := http.NewRequest("POST", url, bytes.NewBuffer(encodedNextData))
			requestNext.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorderNext, requestNext)
			resultNext := recorderNext.Body.String()
			Expect(resultNext).To(Equal(`{"data":{"a":1,"id":778,"name":"B record name"},"status":"OK"}`))
		})
	})
})
