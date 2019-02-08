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
							{
								Name:     "name",
								Type:     description.FieldTypeString,
								Optional: true,
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
								{
									Name:     "name",
									Type:     description.FieldTypeString,
									Optional: true,
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

							aRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "a", map[string]interface{}{"name": "a record"}, auth.User{})
							Expect(err).To(BeNil())

							bRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "b", map[string]interface{}{"name": "b record"}, auth.User{})
							Expect(err).To(BeNil())

							cRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "c", map[string]interface{}{"a": aRecord.Data["id"], "b": bRecord.Data["id"], "name": "c record"}, auth.User{})
							Expect(err).To(BeNil())

							dRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, "d", map[string]interface{}{"a": aRecord.Data["id"], "name": "d record"}, auth.User{})
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
							Expect(body["data"].([]interface{})[0].(map[string]interface{})).NotTo(HaveKey("b"))
						})

						It("Can exclude regular field", func() {
							url := fmt.Sprintf("%s/data/bulk/a?depth=2&exclude=name", appConfig.UrlPrefix)

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
							url := fmt.Sprintf("%s/data/bulk/c?depth=2&exclude=b.name", appConfig.UrlPrefix)

							var request, _ = http.NewRequest("GET", url, nil)
							httpServer.Handler.ServeHTTP(recorder, request)
							responseBody := recorder.Body.String()

							var body map[string]interface{}
							json.Unmarshal([]byte(responseBody), &body)
							Expect(body["data"].([]interface{})).To(HaveLen(1))
							Expect(body["data"].([]interface{})[0].(map[string]interface{})["b"].(map[string]interface{})).NotTo(HaveKey("name"))
						})

						It("Can exclude regular field of outer link", func() {
							url := fmt.Sprintf("%s/data/bulk/a?depth=2&exclude=d_set.name", appConfig.UrlPrefix)

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
							url := fmt.Sprintf("%s/data/bulk/c?depth=1&only=a", appConfig.UrlPrefix)

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
							url := fmt.Sprintf("%s/data/bulk/c?depth=1&only=b.name", appConfig.UrlPrefix)

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
							url := fmt.Sprintf("%s/data/bulk/a?depth=1&only=d_set.name", appConfig.UrlPrefix)

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
							url := fmt.Sprintf("%s/data/bulk/c?depth=1&only=a.id&only=b.id", appConfig.UrlPrefix)

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
							url := fmt.Sprintf("%s/data/bulk/c?depth=2&only=a.id&exclude=b", appConfig.UrlPrefix)

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
							url := fmt.Sprintf("%s/data/bulk/a?depth=1&exclude=d_set", appConfig.UrlPrefix)

							var request, _ = http.NewRequest("GET", url, nil)
							httpServer.Handler.ServeHTTP(recorder, request)
							responseBody := recorder.Body.String()

							var body map[string]interface{}
							json.Unmarshal([]byte(responseBody), &body)
							Expect(body["data"].([]interface{})).To(HaveLen(1))
							Expect(body["data"].([]interface{})[0].(map[string]interface{})).NotTo(HaveKey("d_set"))
						})

						Context("Having an object E with a generic link to object A and a record of object E", func() {
							var eMetaObj *meta.Meta
							var eRecord *record.Record

							BeforeEach(func() {
								var err error

								globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
								Expect(err).To(BeNil())

								metaDescription := description.MetaDescription{
									Name: "e",
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
									},
								}
								eMetaObj, err = metaStore.NewMeta(&metaDescription)
								Expect(err).To(BeNil())
								err = metaStore.Create(globalTransaction, eMetaObj)
								Expect(err).To(BeNil())

								globalTransactionManager.CommitTransaction(globalTransaction)
							})

							BeforeEach(func() {
								globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
								Expect(err).To(BeNil())

								eRecord, err = dataProcessor.CreateRecord(
									globalTransaction.DbTransaction,
									"e",
									map[string]interface{}{"target": map[string]interface{}{"_object": aMetaObj.Name, "id": aRecord.PkAsString()}},
									auth.User{},
								)
								Expect(err).To(BeNil())

								globalTransactionManager.CommitTransaction(globalTransaction)
							})

							It("Can exclude a field of a record which is linked by the generic relation", func() {

								url := fmt.Sprintf("%s/data/bulk/e?depth=2&exclude=target.a.name", appConfig.UrlPrefix)

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
								url := fmt.Sprintf("%s/data/bulk/e?depth=2&exclude=target", appConfig.UrlPrefix)

								var request, _ = http.NewRequest("GET", url, nil)
								httpServer.Handler.ServeHTTP(recorder, request)
								responseBody := recorder.Body.String()

								var body map[string]interface{}
								json.Unmarshal([]byte(responseBody), &body)
								Expect(body["data"].([]interface{})).To(HaveLen(1))
								Expect(body["data"].([]interface{})[0]).NotTo(HaveKey("target"))
							})

							It("Can include a field of a record which is linked by the generic relation", func() {

								url := fmt.Sprintf("%s/data/bulk/e?depth=1&only=target.a.name", appConfig.UrlPrefix)

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

								url := fmt.Sprintf("%s/data/bulk/e?depth=1&only=target.a.name&only=target.a.d_set", appConfig.UrlPrefix)

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

								url := fmt.Sprintf("%s/data/bulk/e?depth=1&only=target&only=target.a", appConfig.UrlPrefix)
								reversedOrderUrl := fmt.Sprintf("%s/data/bulk/e?depth=1&only=target.a&only=target", appConfig.UrlPrefix)

								var request, _ = http.NewRequest("GET", url, nil)
								httpServer.Handler.ServeHTTP(recorder, request)
								responseBody := recorder.Body.String()

								recorder.Body.Reset()
								request, _ = http.NewRequest("GET", reversedOrderUrl, nil)
								httpServer.Handler.ServeHTTP(recorder, request)
								reversedOrderResponseBody := recorder.Body.String()

								Expect(responseBody).To(Equal(reversedOrderResponseBody))
							})
						})
					})
				})
			})
		})
	})
})
