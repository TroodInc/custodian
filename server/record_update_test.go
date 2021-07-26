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

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache())
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
			Name: "a",
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
			Name: "a",
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
				},
				{
					Name:           "b_set",
					Type:           description.FieldTypeArray,
					LinkType:       description.LinkTypeOuter,
					LinkMeta:       "b",
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

	factoryObjectB := func() *object.Meta {
		metaDescription := description.MetaDescription{
			Name: "b",
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
				},
				{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
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

	factoryObjectCWithObjectsLinkToA := func() *object.Meta {
		cMetaDescription := description.MetaDescription{
			Name: "c",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
				{
					Name:     "as",
					Type:     description.FieldTypeObjects,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
				},
			},
		}
		(&description.NormalizationService{}).Normalize(&cMetaDescription)
		cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(cMetaObj)
		Expect(err).To(BeNil())
		return cMetaObj
	}

	It("Cant update M2M field by adding objects", func() {
		aMetaObject := factoryObjectA()
		cMetaObject := factoryObjectCWithObjectsLinkToA()

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
				"name": "root obj m2m test", "as": []interface{}{aRecordFirst.Pk(), aRecordSecond.Pk()},
			},
			auth.User{},
		)
		Expect(err).To(BeNil())

		url := fmt.Sprintf("%s/data/%s/%d", appConfig.UrlPrefix, cMetaObject.Name, int(cRecord.Data["id"].(float64)))

		encodedMetaData, _ := json.Marshal(map[string]interface{}{
			"as": []interface{}{aRecordFirst.Pk(), aRecordSecond.Pk(), aRecordThird.Pk()},
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

	Context("having a record of given object", func() {
		var bRecord *object.Record
		var objectB *object.Meta

		BeforeEach(func() {
			objectA := factoryObjectA()
			objectB = factoryObjectB()
			factoryObjectAWithManuallySetOuterLinkToB()

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

	Context("having a records of given object", func() {
		var aRecord *object.Record
		var bRecord *object.Record
		var cRecord *object.Record
		var objectB *object.Meta
		var err error

		BeforeEach(func() {
			objectA := factoryObjectA()
			objectB = factoryObjectB()
			factoryObjectAWithManuallySetOuterLinkToB()

			aRecord, err = dataProcessor.CreateRecord(objectA.Name, map[string]interface{}{"name": "A record"}, auth.User{})
			Expect(err).To(BeNil())

			bRecord, err = dataProcessor.CreateRecord(objectB.Name, map[string]interface{}{"name": "B record", "a": aRecord.Data["id"]}, auth.User{})
			Expect(err).To(BeNil())

			cRecord, err = dataProcessor.CreateRecord(objectB.Name, map[string]interface{}{"name": "C record", "a": aRecord.Data["id"]}, auth.User{})
			Expect(err).To(BeNil())
		})

		It("updates multiple records and outputs its data respecting depth = 2", func() {
			updateData := []map[string]interface{}{
				{
					"name": "B record new name",
					"id":   bRecord.Data["id"],
				},
				{
					"name": "C record new name",
					"id":   cRecord.Data["id"],
				}}
			encodedMetaData, _ := json.Marshal(updateData)

			url := fmt.Sprintf("%s/data/%s?depth=2", appConfig.UrlPrefix, objectB.Name)
			var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()
			responseStatus := recorder.Code
			Expect(responseStatus).To(Equal(200))
			Expect(responseBody).To(Equal("{\"data\":[{\"a\":{\"b_set\":[1,2],\"id\":1,\"name\":\"A record\"},\"id\":1,\"name\":\"B record new name\"},{\"a\":{\"b_set\":[1,2],\"id\":1,\"name\":\"A record\"},\"id\":2,\"name\":\"C record new name\"}],\"status\":\"OK\",\"total_count\":2}"))
		})
		It("updates multiple records and outputs its data respecting depth = 3", func() {
			updateData := []map[string]interface{}{
				{
					"name": "B record new name",
					"id":   bRecord.Data["id"],
				},
				{
					"name": "C record new name",
					"id":   cRecord.Data["id"],
				}}
			encodedMetaData, _ := json.Marshal(updateData)

			url := fmt.Sprintf("%s/data/%s?depth=3", appConfig.UrlPrefix, objectB.Name)
			var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()
			responseStatus := recorder.Code
			Expect(responseStatus).To(Equal(200))
			Expect(responseBody).To(Equal("{\"data\":[{\"a\":{\"b_set\":[{\"a\":1,\"id\":1,\"name\":\"B record new name\"},{\"a\":1,\"id\":2,\"name\":\"C record new name\"}],\"id\":1,\"name\":\"A record\"},\"id\":1,\"name\":\"B record new name\"},{\"a\":{\"b_set\":[{\"a\":1,\"id\":1,\"name\":\"B record new name\"},{\"a\":1,\"id\":2,\"name\":\"C record new name\"}],\"id\":1,\"name\":\"A record\"},\"id\":2,\"name\":\"C record new name\"}],\"status\":\"OK\",\"total_count\":2}"))
		})
		It("create multiple records and outputs its data respecting depth = 3", func() {
			updateData := []map[string]interface{}{
				{
					"id":   3,
					"name": "D record",
					"a":    aRecord.Data["id"],
				},
				{
					"id":   4,
					"name": "E record",
					"a":    aRecord.Data["id"],
				}}
			encodedMetaData, _ := json.Marshal(updateData)

			url := fmt.Sprintf("%s/data/%s?depth=3", appConfig.UrlPrefix, objectB.Name)
			var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
			request.Header.Set("Content-Type", "application/json")
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()
			responseStatus := recorder.Code
			Expect(responseStatus).To(Equal(200))
			Expect(responseBody).To(Equal("{\"data\":[{\"a\":{\"b_set\":[{\"a\":1,\"id\":1,\"name\":\"B record\"},{\"a\":1,\"id\":2,\"name\":\"C record\"},{\"a\":1,\"id\":3,\"name\":\"D record\"},{\"a\":1,\"id\":4,\"name\":\"E record\"}],\"id\":1,\"name\":\"A record\"},\"id\":3,\"name\":\"D record\"},{\"a\":{\"b_set\":[{\"a\":1,\"id\":1,\"name\":\"B record\"},{\"a\":1,\"id\":2,\"name\":\"C record\"},{\"a\":1,\"id\":3,\"name\":\"D record\"},{\"a\":1,\"id\":4,\"name\":\"E record\"}],\"id\":1,\"name\":\"A record\"},\"id\":4,\"name\":\"E record\"}],\"status\":\"OK\",\"total_count\":2}"))
		})
	})

})
