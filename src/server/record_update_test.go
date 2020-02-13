package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"server/auth"
	"server/data"
	"server/object/meta"
	"server/pg"
	"utils"

	"server"
	"server/data/record"

	pg_transactions "server/pg/transactions"
	"server/transactions"
)

var _ = Describe("Server", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewMetaStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	BeforeEach(func() {
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionUrl).Setup(appConfig)
		recorder = httptest.NewRecorder()
	})

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	factoryObjectA := func() *meta.Meta {
		metaDescription := meta.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*meta.Field{
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
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	factoryObjectAWithManuallySetOuterLinkToB := func(B *meta.Meta) *meta.Meta {
		metaDescription := meta.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*meta.Field{
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
				},
				{
					Name:           "b_set",
					Type:           meta.FieldTypeArray,
					LinkType:       meta.LinkTypeOuter,
					LinkMeta:       B,
					OuterLinkField: B.FindField("a"),
					Optional:       true,
				},
			},
		}
		(&meta.NormalizationService{}).Normalize(&metaDescription)
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(metaObj.Name, metaObj, true)
		Expect(err).To(BeNil())
		return metaObj
	}

	factoryObjectB := func(A *meta.Meta) *meta.Meta {
		metaDescription := meta.Meta{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []*meta.Field{
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
				},
				{
					Name:     "a",
					Type:     meta.FieldTypeObject,
					LinkType: meta.LinkTypeInner,
					LinkMeta: A,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	factoryObjectCWithObjectsLinkToA := func(A *meta.Meta) *meta.Meta {
		cMetaDescription := meta.Meta{
			Name: "c",
			Key:  "id",
			Cas:  false,
			Fields: []*meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "name",
					Type:     meta.FieldTypeString,
					Optional: true,
				},
				{
					Name:     "as",
					Type:     meta.FieldTypeObjects,
					LinkType: meta.LinkTypeInner,
					LinkMeta: A,
				},
			},
		}
		(&meta.NormalizationService{}).Normalize(&cMetaDescription)
		cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(cMetaObj)
		Expect(err).To(BeNil())
		return cMetaObj
	}

	XIt("Cant update M2M field by adding objects", func() {
		aMetaObject := factoryObjectA()
		cMetaObject := factoryObjectCWithObjectsLinkToA(aMetaObject)

		aRecordFirst, err := dataProcessor.CreateRecord(
			aMetaObject.Name,
			map[string]interface{}{"name": "first obj m2m test"}, auth.User{},
		)
		Expect(err).To(BeNil())

		aRecordSecond, err := dataProcessor.CreateRecord(
			aMetaObject.Name,
			map[string]interface{}{"name": "second obj m2m test"}, auth.User{},
		)
		Expect(err).To(BeNil())

		aRecordThird, err := dataProcessor.CreateRecord(
			aMetaObject.Name,
			map[string]interface{}{"name": "third obj m2m test"}, auth.User{},
		)
		Expect(err).To(BeNil())


		cRecord, err := dataProcessor.CreateRecord(
			cMetaObject.Name,
			map[string]interface{}{
				"name": "root obj m2m test", "as":[]interface{}{aRecordFirst.Pk(), aRecordSecond.Pk()},
			},
			auth.User{},
		)
		Expect(err).To(BeNil())

		url := fmt.Sprintf("%s/data/%s/%d", appConfig.UrlPrefix, cMetaObject.Name, int(cRecord.Data["id"].(float64)))

		encodedMetaData, _ := json.Marshal(map[string]interface{}{
			"as":  []interface{}{aRecordFirst.Pk(), aRecordSecond.Pk(), aRecordThird.Pk()},
		})

		var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)

		Expect(recorder.Code).To(Equal(200))

		var body map[string]interface{}
		json.Unmarshal([]byte(recorder.Body.String()), &body)
		Expect(body["data"].(map[string]interface{})["as"]).To(Equal([]interface{}{
			aRecordFirst.Pk(), aRecordSecond.Pk(), aRecordThird.Pk(),
		}))
	})

	It("updates record with the given id, omitting id specified in body", func() {
		Context("having a record of given object", func() {
			objectA := factoryObjectA()

			record, err := dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{"name": "SomeName"}, auth.User{})
			Expect(err).To(BeNil())
			//create another record to ensure only specified record is affected by update
			_, err = dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{"name": "SomeName"}, auth.User{})
			Expect(err).To(BeNil())

			Context("and PUT request performed by URL with specified record ID with wrong id specified in body", func() {
				updateData := map[string]interface{}{
					"name": "SomeOtherName",
					"id":   int(record.Data["id"].(float64) + 1),
				}
				encodedMetaData, _ := json.Marshal(updateData)

				url := fmt.Sprintf("%s/data/%s/%d", appConfig.UrlPrefix, objectA.Name, int(record.Data["id"].(float64)))

				var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
				request.Header.Set("Content-Type", "application/json")
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				Context("response should contain original id and updated name", func() {
					Expect(responseBody).To(Equal("{\"data\":{\"id\":1,\"name\":\"SomeOtherName\"},\"status\":\"OK\"}"))
				})

			})
		})
	})

	XContext("having a record of given object", func() {
		var bRecord *record.Record
		var objectB *meta.Meta

		BeforeEach(func() {
			objectA := factoryObjectA()
			objectB = factoryObjectB(objectA)
			factoryObjectAWithManuallySetOuterLinkToB(objectB)

			aRecord, err := dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())

			bRecord, err = dataProcessor.CreateRecord(objectB.Name, map[string]interface{}{"name": "B record", "a": aRecord.Data["id"]}, auth.User{})
			Expect(err).To(BeNil())
		})

		It("updates record and outputs its data respecting depth", func() {
			updateData := map[string]interface{}{
				"name": "B record new name",
				"id":   bRecord.Data["id"],
			}
			encodedMetaData, _ := json.Marshal(updateData)

			url := fmt.Sprintf("%s/data/%s/%d?depth=2", appConfig.UrlPrefix, objectB.Name, int(bRecord.Data["id"].(float64)))

			var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			Expect(responseBody).To(Equal("{\"data\":{\"a\":{\"b_set\":[1],\"id\":1,\"name\":\"A record\"},\"id\":1,\"name\":\"B record new name\"},\"status\":\"OK\"}"))
		})

		It("updates record and outputs its data respecting depth, omitting field specified in 'exclude' key", func() {
			updateData := map[string]interface{}{
				"name": "B record new name",
				"id":   bRecord.Data["id"],
			}
			encodedMetaData, _ := json.Marshal(updateData)

			url := fmt.Sprintf("%s/data/%s/%d?depth=2,exclude=a", appConfig.UrlPrefix, objectB.Name, int(bRecord.Data["id"].(float64)))

			var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			Expect(responseBody).To(Equal("{\"data\":{\"a\":1,\"id\":1,\"name\":\"B record new name\"},\"status\":\"OK\"}"))
		})
	})
})
