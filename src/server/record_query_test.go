package server_test

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"server/auth"
	"server/data"
	"server/data/record"
	"server/object"
	"server/pg"
	"utils"

	"encoding/json"
	"server"

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

	metaStore := object.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	BeforeEach(func() {
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionUrl).Setup(appConfig)
		recorder = httptest.NewRecorder()
	})

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	makeObjectA := func() *object.Meta {
		metaDescription := object.Meta{
			Name: "a_lxsgk",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
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
				},
				{
					Name:     "description",
					Type:     object.FieldTypeString,
					Optional: true,
				},
			},
		}

		aMetaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())
		return  aMetaObj
	}

	makeObjectD := func(A *object.Meta) *object.Meta {
		dMetaDescription := object.Meta{
			Name: "d_5frz7",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
				{
					Name:     "id",
					Type:     object.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "a",
					LinkMeta: A,
					Type:     object.FieldTypeObject,
					LinkType: object.LinkTypeInner,
					Optional: false,
				},
				{
					Name:     "name",
					Type:     object.FieldTypeString,
					Optional: true,
				},
			},
		}

		dMetaObj, err := metaStore.NewMeta(&dMetaDescription)
		err = metaStore.Create(dMetaObj)
		Expect(err).To(BeNil())
		return dMetaObj
	}

	updateObjctAWithDSet := func(D *object.Meta) *object.Meta {
		aMetaDescription := object.Meta{
			Name: "a_lxsgk",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
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
				},
				{
					Name:           "d_set",
					Type:           object.FieldTypeArray,
					Optional:       true,
					LinkType:       object.LinkTypeOuter,
					LinkMeta:       D,
					OuterLinkField: D.FindField("a"),
					RetrieveMode:   true,
					QueryMode:      true,
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	Context("Having object A", func() {
		var aMetaObj *object.Meta
		BeforeEach(func() {
			aMetaObj = makeObjectA()
		})

		It("returns all records including total count", func() {
			for i := 0; i < 50; i++ {
				_, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A record"}, auth.User{})
				Expect(err).To(BeNil())
			}

			url := fmt.Sprintf("%s/data/%s?depth=1", appConfig.UrlPrefix, aMetaObj.Name)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(50))
			Expect(body["total_count"].(float64)).To(Equal(float64(50)))
		})

		It("returns slice of records including total count", func() {
			for i := 0; i < 50; i++ {
				_, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A record"}, auth.User{})
				Expect(err).To(BeNil())
			}

			url := fmt.Sprintf("%s/data/%s?depth=1&q=limit(0,10)", appConfig.UrlPrefix, aMetaObj.Name)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(10))
			Expect(body["total_count"].(float64)).To(Equal(float64(50)))
		})

		It("returns empty list including total count", func() {

			url := fmt.Sprintf("%s/data/%s?depth=1", appConfig.UrlPrefix, aMetaObj.Name)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(0))
			Expect(body["total_count"].(float64)).To(Equal(float64(0)))
		})

		It("returns records by query including total count", func() {
			for i := 0; i < 20; i++ {
				_, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A"}, auth.User{})
				Expect(err).To(BeNil())
			}
			for i := 0; i < 20; i++ {
				_, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "B"}, auth.User{})
				Expect(err).To(BeNil())
			}

			url := fmt.Sprintf("%s/data/%s?depth=1&q=eq(name,B),limit(0,5)", appConfig.UrlPrefix, aMetaObj.Name)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(5))
			Expect(body["total_count"].(float64)).To(Equal(float64(20)))
		})
	})

	Context("Having records of objects A,B,C,D", func() {
		var aRecord, bRecord, cRecord, dRecord *record.Record
		BeforeEach(func() {
			aMetaObj := makeObjectA()
			bMetaDescription := object.Meta{
				Name: "b_atzw9",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
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
					},
					{
						Name:     "description",
						Type:     object.FieldTypeString,
						Optional: true,
					},
				},
			}
			bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)

			cMetaDescription := object.Meta{
				Name: "c_s7ohu",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
					{
						Name:     "id",
						Type:     object.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name:     "b",
						Type:     object.FieldTypeObject,
						LinkType: object.LinkTypeInner,
						LinkMeta: bMetaObj,
						Optional: false,
					},
					{
						Name:     "a",
						Type:     object.FieldTypeObject,
						LinkType: object.LinkTypeInner,
						LinkMeta: aMetaObj,
						Optional: false,
					},
					{
						Name:     "name",
						Type:     object.FieldTypeString,
						Optional: true,
					},
				},
			}
			cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
			err = metaStore.Create(cMetaObj)

			dMetaObj := makeObjectD(aMetaObj)

			aMetaObj = updateObjctAWithDSet(dMetaObj)

			aRecord, err = dataProcessor.CreateRecord( aMetaObj.Name, map[string]interface{}{"name": "a record"}, auth.User{})
			Expect(err).To(BeNil())

			bRecord, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "b record"}, auth.User{})
			Expect(err).To(BeNil())

			cRecord, err = dataProcessor.CreateRecord(cMetaObj.Name, map[string]interface{}{"a": aRecord.Data["id"], "b": bRecord.Data["id"], "name": "c record"}, auth.User{})
			Expect(err).To(BeNil())

			dRecord, err = dataProcessor.CreateRecord(dMetaObj.Name, map[string]interface{}{"a": aRecord.Data["id"], "name": "d record"}, auth.User{})
			Expect(err).To(BeNil())
		})

		It("Can exclude inner link`s subtree", func() {
			url := fmt.Sprintf("%s/data/c_s7ohu?depth=2&exclude=b", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})).NotTo(HaveKey("b"))
		})

		It("Can exclude regular field", func() {
			url := fmt.Sprintf("%s/data/a_lxsgk?depth=2&exclude=name", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})["id"].(float64)).To(Equal(aRecord.Data["id"]))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})["d_set"]).NotTo(BeNil())
		})

		It("Can exclude regular field of inner link", func() {
			url := fmt.Sprintf("%s/data/c_s7ohu?depth=2&exclude=b.name", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})["b"].(map[string]interface{})).NotTo(HaveKey("name"))
		})

		It("Can exclude regular field of outer link", func() {
			url := fmt.Sprintf("%s/data/a_lxsgk?depth=2&exclude=d_set.name", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			dSet := body["data"].([]interface{})[0].(map[string]interface{})["d_set"].([]interface{})
			Expect(dSet[0].(map[string]interface{})).NotTo(HaveKey("name"))
		})

		It("Can include inner link as key value", func() {
			url := fmt.Sprintf("%s/data/c_s7ohu?depth=1&only=a", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})).NotTo(HaveKey("b"))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})["a"].(float64)).To(Equal(aRecord.Data["id"]))
		})

		It("Can include inner link`s field", func() {
			url := fmt.Sprintf("%s/data/c_s7ohu?depth=1&only=b.name", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			bData := body["data"].([]interface{})[0].(map[string]interface{})["b"].(map[string]interface{})
			Expect(bData["id"]).To(Equal(bRecord.Data["id"]))
			Expect(bData).To(HaveKey("name"))
			Expect(bData).NotTo(HaveKey("description"))
		})

		It("Can exclude regular field of outer link", func() {
			url := fmt.Sprintf("%s/data/a_lxsgk?depth=1&only=d_set.name", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			dSet := body["data"].([]interface{})[0].(map[string]interface{})["d_set"].([]interface{})
			Expect(dSet[0].(map[string]interface{})).To(HaveKey("name"))
			Expect(dSet[0].(map[string]interface{})).To(HaveKey("id"))
		})

		It("Can include more than one inner link`s subtrees together", func() {
			url := fmt.Sprintf("%s/data/c_s7ohu?depth=1&only=a.id&only=b.id", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})["b"].(map[string]interface{})["id"]).To(Equal(bRecord.Data["id"]))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})["a"].(map[string]interface{})["id"]).To(Equal(aRecord.Data["id"]))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})["name"].(string)).To(Equal(cRecord.Data["name"]))
		})

		It("Can include and exclude fields at once", func() {
			url := fmt.Sprintf("%s/data/c_s7ohu?depth=2&only=a.id&exclude=b", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})).NotTo(HaveKey("b"))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})["a"].(map[string]interface{})["id"]).To(Equal(aRecord.Data["id"]))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})["name"].(string)).To(Equal(cRecord.Data["name"]))
		})

		It("Can exclude outer link`s tree", func() {
			url := fmt.Sprintf("%s/data/a_lxsgk?depth=1&exclude=d_set", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			Expect(body["data"].([]interface{})[0].(map[string]interface{})).NotTo(HaveKey("d_set"))
		})
	})

	Context("Having an object E with a generic link to object A and a record of object E", func() {
		var aRecord *record.Record

		BeforeEach(func() {
			aMetaObj := makeObjectA()
			dMetaObj := makeObjectD(aMetaObj)
			aMetaObj = updateObjctAWithDSet(dMetaObj)

			metaDescription := object.Meta{
				Name: "e_m7o1b",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
					{
						Name: "id",
						Type: object.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
					{
						Name:         "target",
						Type:         object.FieldTypeGeneric,
						LinkType:     object.LinkTypeInner,
						LinkMetaList: []*object.Meta{aMetaObj},
						Optional:     false,
					},
					{
						Name:     "name",
						Type:     object.FieldTypeString,
						Optional: true,
					},
				},
			}
			eMetaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(eMetaObj)
			Expect(err).To(BeNil())

			aRecord, err = dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "a record"}, auth.User{})
			Expect(err).To(BeNil())

			_, err = dataProcessor.CreateRecord(
				eMetaObj.Name,
				map[string]interface{}{"target": map[string]interface{}{"_object": aMetaObj.Name, "id": aRecord.PkAsString()}},
				auth.User{},
			)
			Expect(err).To(BeNil())
		})

		It("Can exclude a field of a record which is linked by the generic relation", func() {

			url := fmt.Sprintf("%s/data/e_m7o1b?depth=2&exclude=target.a_lxsgk.name", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			targetData := body["data"].([]interface{})[0].(map[string]interface{})["target"].(map[string]interface{})
			Expect(targetData).NotTo(HaveKey("name"))

		})

		It("Can exclude a record which is linked by the generic relation", func() {
			url := fmt.Sprintf("%s/data/e_m7o1b?depth=2&exclude=target", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			Expect(body["data"].([]interface{})[0]).NotTo(HaveKey("target"))
		})

		It("Can include a field of a record which is linked by the generic relation", func() {

			url := fmt.Sprintf("%s/data/e_m7o1b?depth=1&only=target.a_lxsgk.name", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			targetData := body["data"].([]interface{})[0].(map[string]interface{})["target"].(map[string]interface{})
			Expect(targetData).To(HaveKey("name"))
			Expect(targetData).NotTo(HaveKey("d_set"))
			Expect(targetData["name"].(string)).To(Equal(aRecord.Data["name"]))
		})

		It("Can include a field of a record which is linked by the generic relation and its nested item at once", func() {

			url := fmt.Sprintf("%s/data/e_m7o1b?depth=1&only=target.a_lxsgk.name&only=target.a_lxsgk.d_set", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			targetData := body["data"].([]interface{})[0].(map[string]interface{})["target"].(map[string]interface{})
			Expect(targetData).To(HaveKey("name"))
			Expect(targetData).To(HaveKey("d_set"))
		})

		It("Applies policies regardless of specification`s order in query", func() {

			url := fmt.Sprintf("%s/data/e_m7o1b?depth=1&only=target&only=target.a", appConfig.UrlPrefix)
			reversedOrderUrl := fmt.Sprintf("%s/data/e_m7o1b?depth=1&only=target.a&only=target", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			recorder.Body.Reset()
			request, _ = http.NewRequest("GET", reversedOrderUrl, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			reversedOrderResponseBody := recorder.Body.String()

			Expect(responseBody).To(Equal(reversedOrderResponseBody))
		})

		It("Can include a generic field", func() {

			url := fmt.Sprintf("%s/data/e_m7o1b?depth=1&only=target", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			recordData := body["data"].([]interface{})[0].(map[string]interface{})
			Expect(recordData).To(HaveKey("target"))
			Expect(recordData).To(HaveKey("id"))
			Expect(recordData).NotTo(HaveKey("name"))
		})

		It("Can include a generic field and its subtree", func() {

			url := fmt.Sprintf("%s/data/e_m7o1b?depth=1&only=target&only=target.a_lxsgk.name", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))

			recordData := body["data"].([]interface{})[0].(map[string]interface{})
			Expect(recordData).To(HaveKey("target"))
			Expect(recordData).To(HaveKey("id"))
			Expect(recordData).NotTo(HaveKey("name"))

			targetData := recordData["target"].(map[string]interface{})
			Expect(targetData).NotTo(HaveKey("description"))
			Expect(targetData).To(HaveKey("name"))
			Expect(targetData).To(HaveKey("id"))
		})
	})
})
