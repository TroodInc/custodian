package server_test

import (
	"custodian/server/auth"
	"custodian/server/object"
	"custodian/utils"
	"fmt"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"custodian/server"
	"custodian/server/object/description"

	"encoding/json"
)

var _ = Describe("Server 101", func() {
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

	makeObjectA := func() *object.Meta {
		metaDescription := description.MetaDescription{
			Name: "a_lxsgk",
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
					Name:     "description",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}

		aMetaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())
		return aMetaObj
	}

	makeObjectD := func() *object.Meta {
		dMetaDescription := description.MetaDescription{
			Name: "d_5frz7",
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
					Name:     "a",
					LinkMeta: "a_lxsgk",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					Optional: false,
				},
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}

		dMetaObj, err := metaStore.NewMeta(&dMetaDescription)
		err = metaStore.Create(dMetaObj)
		Expect(err).To(BeNil())
		return dMetaObj
	}

	updateObjctAWithDSet := func() *object.Meta {
		aMetaDescription := description.MetaDescription{
			Name: "a_lxsgk",
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
					Name:           "d_set",
					Type:           description.FieldTypeArray,
					Optional:       true,
					LinkType:       description.LinkTypeOuter,
					LinkMeta:       "d_5frz7",
					OuterLinkField: "a",
					RetrieveMode:   true,
					QueryMode:      true,
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		_, err = metaStore.Update(aMetaObj.Name, aMetaObj, true, true)
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

		It("returns same records using different queries", func() {
			for i := 0; i < 20; i++ {
				_, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "A"}, auth.User{})
				Expect(err).To(BeNil())
			}

			url := fmt.Sprintf("%s/data/%s?q=eq(name,A),gt(id,5),limit(0,10)", appConfig.UrlPrefix, aMetaObj.Name)
			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(10))
			Expect(body["total_count"].(float64)).To(Equal(float64(15)))

			url = fmt.Sprintf("%s/data/%s?q=eq(name,A)&q=gt(id,5)&q=limit(0,10)", appConfig.UrlPrefix, aMetaObj.Name)
			request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody = recorder.Body.String()

			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(10))
			Expect(body["total_count"].(float64)).To(Equal(float64(15)))
		})
	})

	Context("Having records of objects A,B,C,D", func() {
		var aRecord, bRecord, cRecord *object.Record
		BeforeEach(func() {
			aMetaObj := makeObjectA()
			bMetaDescription := description.MetaDescription{
				Name: "b_atzw9",
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
						Name:     "description",
						Type:     description.FieldTypeString,
						Optional: true,
					},
				},
			}
			bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMetaObj)

			cMetaDescription := description.MetaDescription{
				Name: "c_s7ohu",
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
						Name:     "b",
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: bMetaObj.Name,
						Optional: false,
					},
					{
						Name:     "a",
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						LinkMeta: aMetaObj.Name,
						Optional: false,
					},
					{
						Name:     "name",
						Type:     description.FieldTypeString,
						Optional: true,
					},
				},
			}
			cMetaObj, err := metaStore.NewMeta(&cMetaDescription)
			err = metaStore.Create(cMetaObj)

			dMetaObj := makeObjectD()

			aMetaObj = updateObjctAWithDSet()

			aRecord, err = dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{"name": "a record"}, auth.User{})
			Expect(err).To(BeNil())

			bRecord, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{"name": "b record"}, auth.User{})
			Expect(err).To(BeNil())

			cRecord, err = dataProcessor.CreateRecord(cMetaObj.Name, map[string]interface{}{"a": aRecord.Data["id"], "b": bRecord.Data["id"], "name": "c record"}, auth.User{})
			Expect(err).To(BeNil())

			_, err = dataProcessor.CreateRecord(dMetaObj.Name, map[string]interface{}{"a": aRecord.Data["id"], "name": "d record"}, auth.User{})
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

		It("Can include two fields of one object", func() {
			url := fmt.Sprintf("%s/data/c_s7ohu?depth=2&only=b.description&only=b.name", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			bData := body["data"].([]interface{})[0].(map[string]interface{})["b"].(map[string]interface{})
			Expect(bData).To(HaveKey("name"))
			Expect(bData).To(HaveKey("description"))
		})

		It("Can include one field", func() {
			url := fmt.Sprintf("%s/data/c_s7ohu?depth=2&only=b.description", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			bData := body["data"].([]interface{})[0].(map[string]interface{})["b"].(map[string]interface{})
			Expect(bData).NotTo(HaveKey("name"))
			Expect(bData).To(HaveKey("description"))
		})

		It("Can include 123 object and two fields of different objects", func() {
			url := fmt.Sprintf("%s/data/c_s7ohu?depth=2&only=b.name&only=a.name", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			aData := body["data"].([]interface{})[0].(map[string]interface{})["a"].(map[string]interface{})
			bData := body["data"].([]interface{})[0].(map[string]interface{})["b"].(map[string]interface{})
			Expect(bData).To(HaveKey("name"))
			Expect(bData).NotTo(HaveKey("description"))
			Expect(aData).To(HaveKey("name"))
			Expect(aData).NotTo(HaveKey("d_set"))

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
		var aRecord *object.Record

		BeforeEach(func() {
			aMetaObj := makeObjectA()
			makeObjectD()
			aMetaObj = updateObjctAWithDSet()

			metaDescription := description.MetaDescription{
				Name: "e_m7o1b",
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
						Name:         "target",
						Type:         description.FieldTypeGeneric,
						LinkType:     description.LinkTypeInner,
						LinkMetaList: []string{aMetaObj.Name},
						Optional:     false,
					},
					{
						Name:     "name",
						Type:     description.FieldTypeString,
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
	Context("Having an object with objects link", func() {
		testObjAName := fmt.Sprintf("%s_a", utils.RandomString(8))
		testObjBName := fmt.Sprintf("%s_b", utils.RandomString(8))
		havingObjectA := func() *object.Meta {
			bMetaDescription := description.MetaDescription{
				Name: testObjAName,
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
						Name:     "description",
						Type:     description.FieldTypeString,
						Optional: true,
					},
					{
						Name:     "contact",
						Type:     description.FieldTypeString,
						Optional: true,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&bMetaDescription)
			(&description.NormalizationService{}).Normalize(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())
			return metaObj
		}

		havingObjectB := func() *object.Meta {
			metaDescription := description.MetaDescription{
				Name: testObjBName,
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
						Name:     "as",
						Type:     description.FieldTypeObjects,
						LinkMeta: testObjAName,
						LinkType: description.LinkTypeInner,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())
			return metaObj
		}

		BeforeEach(func() {
			havingObjectA()
			havingObjectB()

			aRecord, err := dataProcessor.CreateRecord(testObjAName, map[string]interface{}{"name": "a record", "description": "a record description", "contact": "a record contact"}, auth.User{})
			Expect(err).To(BeNil())

			_, err = dataProcessor.CreateRecord(
				testObjBName,
				map[string]interface{}{"as": []interface{}{aRecord.Pk()}, "name": "b reord name"},
				auth.User{},
			)
			Expect(err).To(BeNil())
		})

		It("Can include only m2m field", func() {
			url := fmt.Sprintf("%s/data/%s?depth=1&only=as", appConfig.UrlPrefix, testObjBName)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))

			recordData := body["data"].([]interface{})[0].(map[string]interface{})
			Expect(recordData).To(HaveKey("as"))
			Expect(recordData).To(HaveKey("id"))
			Expect(recordData).NotTo(HaveKey("name"))
		})

		It("Can include two filelds of m2m object", func() {
			url := fmt.Sprintf("%s/data/%s?depth=1&only=as.name&&only=as.description", appConfig.UrlPrefix, testObjBName)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))

			recordData := body["data"].([]interface{})[0].(map[string]interface{})
			Expect(recordData).To(HaveKey("as"))
			Expect(recordData).To(HaveKey("id"))

			m2mRecordData := recordData["as"].([]interface{})[0].(map[string]interface{})
			Expect(m2mRecordData).To(HaveKey("name"))
			Expect(m2mRecordData).To(HaveKey("description"))
		})

		It("Can exclude fileld of m2m object", func() {
			url := fmt.Sprintf("%s/data/%s?depth=2&exclude=as.name", appConfig.UrlPrefix, testObjBName)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(1))

			recordData := body["data"].([]interface{})[0].(map[string]interface{})
			Expect(recordData).To(HaveKey("as"))
			Expect(recordData).To(HaveKey("id"))

			m2mRecordData := recordData["as"].([]interface{})[0].(map[string]interface{})
			Expect(m2mRecordData).NotTo(HaveKey("name"))
		})

	})
})
