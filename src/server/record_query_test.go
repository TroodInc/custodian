package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"fmt"
	"net/http/httptest"
	"server/pg"
	"server/data"
	"server/auth"
	"utils"
	"server/transactions/file_transaction"

	"server/object/meta"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/object/description"
	"server"
	"encoding/json"
	"server/data/record"
)

var _ = Describe("Server", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer)

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	var globalTransaction *transactions.GlobalTransaction

	BeforeEach(func() {
		var err error

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionOptions).Setup(appConfig)
		recorder = httptest.NewRecorder()

	})

	AfterEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	Context("Having object A", func() {
		var aMetaObj *meta.Meta
		BeforeEach(func() {
			var err error
			globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

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
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
				},
			}
			aMetaObj, err = metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, aMetaObj)
			Expect(err).To(BeNil())

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("returns all records including total count", func() {

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			for i := 0; i < 50; i++ {
				_, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaObj.Name, map[string]interface{}{"name": "A record"}, auth.User{})
				Expect(err).To(BeNil())
			}

			globalTransactionManager.CommitTransaction(globalTransaction)

			url := fmt.Sprintf("%s/data/bulk/%s?depth=1", appConfig.UrlPrefix, aMetaObj.Name)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(50))
			Expect(body["total_count"].(float64)).To(Equal(float64(50)))
		})

		It("returns slice of records including total count", func() {

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())
			for i := 0; i < 50; i++ {
				_, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaObj.Name, map[string]interface{}{"name": "A record"}, auth.User{})
				Expect(err).To(BeNil())
			}

			globalTransactionManager.CommitTransaction(globalTransaction)

			url := fmt.Sprintf("%s/data/bulk/%s?depth=1&q=limit(0,10)", appConfig.UrlPrefix, aMetaObj.Name)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(10))
			Expect(body["total_count"].(float64)).To(Equal(float64(50)))
		})

		It("returns empty list including total count", func() {

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			globalTransactionManager.CommitTransaction(globalTransaction)

			url := fmt.Sprintf("%s/data/bulk/%s?depth=1", appConfig.UrlPrefix, aMetaObj.Name)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(0))
			Expect(body["total_count"].(float64)).To(Equal(float64(0)))
		})

		It("returns records by query including total count", func() {

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			for i := 0; i < 20; i++ {
				_, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaObj.Name, map[string]interface{}{"name": "A"}, auth.User{})
				Expect(err).To(BeNil())
			}
			for i := 0; i < 20; i++ {
				_, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaObj.Name, map[string]interface{}{"name": "B"}, auth.User{})
				Expect(err).To(BeNil())
			}

			globalTransactionManager.CommitTransaction(globalTransaction)

			url := fmt.Sprintf("%s/data/bulk/%s?depth=1&q=eq(name,B),limit(0,5)", appConfig.UrlPrefix, aMetaObj.Name)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)
			responseBody := recorder.Body.String()

			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)
			Expect(body["data"].([]interface{})).To(HaveLen(5))
			Expect(body["total_count"].(float64)).To(Equal(float64(20)))
		})

		Context("Having object B", func() {
			var bMetaObj *meta.Meta
			BeforeEach(func() {
				var err error
				globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				bMetaDescription := description.MetaDescription{
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
					},
				}
				bMetaObj, err = metaStore.NewMeta(&bMetaDescription)
				Expect(err).To(BeNil())
				err = metaStore.Create(globalTransaction, bMetaObj)
				Expect(err).To(BeNil())

				globalTransactionManager.CommitTransaction(globalTransaction)
			})

			Context("Having object C, which has a link to the objects B and A", func() {
				var cMetaObj *meta.Meta
				BeforeEach(func() {
					var err error
					globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
					Expect(err).To(BeNil())

					cMetaDescription := description.MetaDescription{
						Name: "c",
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
								LinkMeta: "b",
								Optional: false,
							},
							{
								Name:     "a",
								Type:     description.FieldTypeObject,
								LinkType: description.LinkTypeInner,
								LinkMeta: "a",
								Optional: false,
							},
						},
					}
					cMetaObj, err = metaStore.NewMeta(&cMetaDescription)
					Expect(err).To(BeNil())
					err = metaStore.Create(globalTransaction, cMetaObj)
					Expect(err).To(BeNil())

					globalTransactionManager.CommitTransaction(globalTransaction)
				})

				Context("Having object D, which has a link to the object A, which has an explicit outer link to D", func() {
					var dMetaObj *meta.Meta
					BeforeEach(func() {
						var err error

						globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
						Expect(err).To(BeNil())

						dMetaDescription := description.MetaDescription{
							Name: "d",
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
									LinkMeta: "a",
									Type:     description.FieldTypeObject,
									LinkType: description.LinkTypeInner,
									Optional: false,
								},
							},
						}
						dMetaObj, err = metaStore.NewMeta(&dMetaDescription)
						Expect(err).To(BeNil())
						err = metaStore.Create(globalTransaction, dMetaObj)
						Expect(err).To(BeNil())

						aMetaDescription := description.MetaDescription{
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
									Def: map[string]interface{}{
										"func": "nextval",
									},
								},
								{
									Name:           "d_set",
									Type:           description.FieldTypeArray,
									Optional:       true,
									LinkType:       description.LinkTypeOuter,
									LinkMeta:       "d",
									OuterLinkField: "a",
									RetrieveMode:   true,
									QueryMode:      true,
								},
							},
						}
						aMetaObj, err = metaStore.NewMeta(&aMetaDescription)
						Expect(err).To(BeNil())
						_, err = metaStore.Update(globalTransaction, aMetaObj.Name, aMetaObj, true)
						Expect(err).To(BeNil())

						globalTransactionManager.CommitTransaction(globalTransaction)
					})

					Context("Having records of objects A,B,C,D", func() {
						var aRecord, bRecord, cRecord, dRecord *record.Record
						BeforeEach(func() {
							var err error

							globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
							Expect(err).To(BeNil())

							aRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "a", map[string]interface{}{}, auth.User{})
							Expect(err).To(BeNil())

							bRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "b", map[string]interface{}{}, auth.User{})
							Expect(err).To(BeNil())

							cRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "c", map[string]interface{}{"a": aRecord.Data["id"], "b": bRecord.Data["id"]}, auth.User{})
							Expect(err).To(BeNil())

							dRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "d", map[string]interface{}{"a": aRecord.Data["id"]}, auth.User{})
							Expect(err).To(BeNil())

							globalTransactionManager.CommitTransaction(globalTransaction)
						})

						It("Can exclude inner link`s subtree", func() {
							url := fmt.Sprintf("%s/data/bulk/c?depth=2&exclude=b", appConfig.UrlPrefix)

							var request, _ = http.NewRequest("GET", url, nil)
							httpServer.Handler.ServeHTTP(recorder, request)
							responseBody := recorder.Body.String()

							var body map[string]interface{}
							json.Unmarshal([]byte(responseBody), &body)
							Expect(body["data"].([]interface{})).To(HaveLen(1))
							Expect(body["data"].([]interface{})[0].(map[string]interface{})["b"].(float64)).To(Equal(bRecord.Data["id"]))
						})

						It("Can include inner link`s subtree", func() {
							url := fmt.Sprintf("%s/data/bulk/c?depth=1&include=a", appConfig.UrlPrefix)

							var request, _ = http.NewRequest("GET", url, nil)
							httpServer.Handler.ServeHTTP(recorder, request)
							responseBody := recorder.Body.String()

							var body map[string]interface{}
							json.Unmarshal([]byte(responseBody), &body)
							Expect(body["data"].([]interface{})).To(HaveLen(1))
							Expect(body["data"].([]interface{})[0].(map[string]interface{})["b"].(float64)).To(Equal(bRecord.Data["id"]))
							Expect(body["data"].([]interface{})[0].(map[string]interface{})["a"].(map[string]interface{})["id"]).To(Equal(aRecord.Data["id"]))
						})

						It("Can include more than one inner link`s subtrees together", func() {
							url := fmt.Sprintf("%s/data/bulk/c?depth=1&include=a&include=b", appConfig.UrlPrefix)

							var request, _ = http.NewRequest("GET", url, nil)
							httpServer.Handler.ServeHTTP(recorder, request)
							responseBody := recorder.Body.String()

							var body map[string]interface{}
							json.Unmarshal([]byte(responseBody), &body)
							Expect(body["data"].([]interface{})).To(HaveLen(1))
							Expect(body["data"].([]interface{})[0].(map[string]interface{})["b"].(map[string]interface{})["id"]).To(Equal(bRecord.Data["id"]))
							Expect(body["data"].([]interface{})[0].(map[string]interface{})["a"].(map[string]interface{})["id"]).To(Equal(aRecord.Data["id"]))
						})

						It("Can exclude outer link`s tree", func() {
							url := fmt.Sprintf("%s/data/bulk/a?depth=1&exclude=d_set", appConfig.UrlPrefix)

							var request, _ = http.NewRequest("GET", url, nil)
							httpServer.Handler.ServeHTTP(recorder, request)
							responseBody := recorder.Body.String()

							var body map[string]interface{}
							json.Unmarshal([]byte(responseBody), &body)
							Expect(body["data"].([]interface{})).To(HaveLen(1))
							Expect(body["data"].([]interface{})[0].(map[string]interface{})).NotTo(HaveKey("d_set"))
						})
					})
				})
			})
		})
	})
})
