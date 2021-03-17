package server_test

import (
	"custodian/server/abac"
	"custodian/server/object"
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"custodian/server/object/meta"
	"custodian/utils"

	"bytes"
	"custodian/server"
	"custodian/server/auth"
	"custodian/server/data"
	"custodian/server/data/record"
	"custodian/server/object/description"
	"custodian/server/transactions"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
)

const SERVICE_DOMAIN = "custodian"

func get_server(user *auth.User) *http.Server {
	os.Setenv("SERVICE_DOMAIN", SERVICE_DOMAIN)

	appConfig := utils.GetConfig()
	custodianServer := server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionUrl)

	if user != nil {
		custodianServer.SetAuthenticator(auth.NewMockAuthenticator(*user))
	}

	return custodianServer.Setup(appConfig)
}

var _ = Describe("ABAC rules handling", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(dbTransactionManager)
	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(globalTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	testObjName := utils.RandomString(8)

	var user *auth.User

	flushDb := func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	}

	factoryObjectA := func() *meta.Meta {
		metaDescription := description.MetaDescription{
			Name: testObjName,
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
					Name:     "owner_role",
					Type:     description.FieldTypeString,
					Optional: true,
				},
				{
					Name:     "color",
					Type:     description.FieldTypeString,
					Optional: true,
					Def:      "red",
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	JustBeforeEach(func() {
		flushDb()
		recorder = httptest.NewRecorder()
	})

	AfterEach(flushDb)

	Describe("'Subject' scope tests", func() {
		BeforeEach(func() {
			user = &auth.User{
				Role: abac.JsonToObject(`{"id": "admin"}`),
				ABAC: map[string]interface{}{
					"_default_resolution": "deny",
					SERVICE_DOMAIN: map[string]interface{}{
						testObjName: map[string]interface{}{
							"data_GET": []interface{}{
								map[string]interface{}{
									"result": "allow",
									"rule": map[string]interface{}{
										"sbj.role.id": map[string]interface{}{"eq": "admin"},
									},
								},
							},
						},
						"meta": map[string]interface{}{
							"*": []interface{}{
								map[string]interface{}{
									"result": "allow",
									"rule": map[string]interface{}{
										"sbj.role.id": map[string]interface{}{"eq": "admin"},
									},
								},
							},
							"GET": []interface{}{
								map[string]interface{}{
									"result": "allow",
									"rule": map[string]interface{}{
										"sbj.role.id": map[string]interface{}{"eq": "manager"},
									},
								},
							},
						},
					},
				},
			}
		})

		Context("Unauthorized User", func() {
			It("Must deny unauthorized", func() {
				user = &auth.User{
					Authorized: false,
					ABAC: map[string]interface{}{
						"_default_resolution": "allow",
						SERVICE_DOMAIN: map[string]interface{}{
							testObjName: map[string]interface{}{
								"*": []interface{}{
									map[string]interface{}{
										"result": "deny",
										"rule": map[string]interface{}{
											"sbj.authorized": map[string]interface{}{"eq": false},
										},
									},
								},
							},
						},
					},
				}

				httpServer = get_server(user)
				factoryObjectA()

				url := fmt.Sprintf("%s/data/a", appConfig.UrlPrefix)

				var request, _ = http.NewRequest("GET", url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("FAIL"))
			})
		})

		Context("NOT IN can work together", func() {
			It("NOT IN can work together", func() {
				user = &auth.User{
					Role: abac.JsonToObject(`{"id": "admin"}`),
					ABAC: abac.JsonToObject(fmt.Sprintf(`
					{
						"_default_resolution": "allow",
						"%s": {
							"%s": {
								"data_GET": [
									{
										"result": "deny",
										"rule": {
											"sbj.role.id": {
												"not": {
													"in": [
														"manager",
														"admin"
													]
												}
											}
										}
									}
								]
							}
						}
					}`, SERVICE_DOMAIN, testObjName)),
				}

				httpServer = get_server(user)
				factoryObjectA()

				url := fmt.Sprintf("%s/data/%s", appConfig.UrlPrefix, testObjName)

				var request, _ = http.NewRequest("GET", url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("OK"))
			})
		})

		Context("NOT operator can work with numeric id allowed", func() {
			It("NOT operator can work with numeric id", func() {
				url := fmt.Sprintf("%s/data/%s", appConfig.UrlPrefix, testObjName)

				user := &auth.User{
					Role: abac.JsonToObject(`{"id": 20}`),
					ABAC: abac.JsonToObject(fmt.Sprintf(`
					{
						"_default_resolution": "allow",
						"%s": {
							"%s": {
								"data_GET": [
									{
										"result": "deny",
										"rule": {
											"sbj.role.id": {
												"not": 21
											}
										}
									}
								]
							}
						}
					}`, SERVICE_DOMAIN, testObjName)),
				}

				httpServer = get_server(user)
				factoryObjectA()

				request, _ := http.NewRequest("GET", url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("FAIL"))

			})
		})

		Context("NOT operator can work with numeric id allowed", func() {
			It("NOT operator can work with numeric id", func() {
				url := fmt.Sprintf("%s/data/%s", appConfig.UrlPrefix, testObjName)

				user := &auth.User{
					Role: abac.JsonToObject(`{"id": 21}`),
					ABAC: abac.JsonToObject(fmt.Sprintf(`
					{
						"_default_resolution": "allow",
						"%s": {
							"%s": {
								"data_GET": [
									{
										"result": "deny",
										"rule": {
											"sbj.role.id": {
												"not": 21
											}
										}
									}
								]
							}
						}
					}`, SERVICE_DOMAIN, testObjName)),
				}

				httpServer = get_server(user)
				factoryObjectA()

				request, _ := http.NewRequest("GET", url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("OK"))

			})
		})
		Context("Meta & Wildcard rules", func() {
			It("Must allow meta list with ACTION rule set", func() {
				user.Role = abac.JsonToObject(`{"id": "manager"}`)
				httpServer = get_server(user)
				factoryObjectA()

				url := fmt.Sprintf("%s/meta", appConfig.UrlPrefix)

				var request, _ = http.NewRequest("GET", url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("OK"))
			})

			It("Must deny meta Creation", func() {
				user.Role = abac.JsonToObject(`{"id": "manager"}`)
				httpServer = get_server(user)
				encodedMetaData := []byte(`{"name":"test","key":"id","cas":false,"fields":[{"name":"id","type":"number","optional":true}]}`)

				url := fmt.Sprintf("%s/meta", appConfig.UrlPrefix)

				var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("FAIL"))
			})

			It("Must deny meta list for Unauthorized", func() {
				user.Role = nil
				user.Authorized = false

				httpServer = get_server(user)
				factoryObjectA()

				url := fmt.Sprintf("%s/meta", appConfig.UrlPrefix)

				var request, _ = http.NewRequest("GET", url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("FAIL"))
			})

			It("Must allow meta list by wildcard", func() {
				user.Role = abac.JsonToObject(`{"id": "admin"}`)
				httpServer = get_server(user)
				factoryObjectA()

				url := fmt.Sprintf("%s/meta", appConfig.UrlPrefix)

				var request, _ = http.NewRequest("GET", url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("OK"))
			})
		})

		Context("And this user has the role 'manager'", func() {
			It("Should return error when trying to retrieve a record of object A", func() {
				user.Role = abac.JsonToObject(`{"id": "manager"}`)
				httpServer = get_server(user)
				aObject := factoryObjectA()

				aRecord, err := dataProcessor.CreateRecord(aObject.Name, map[string]interface{}{"name": "A record"}, auth.User{})
				Expect(err).To(BeNil())

				url := fmt.Sprintf("%s/data/%s/%s", appConfig.UrlPrefix, aObject.Name, aRecord.PkAsString())

				var request, _ = http.NewRequest("GET", url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("FAIL"))
			})
		})

		Context("And this user has the role 'admin'", func() {
			It("Should return a record of object A", func() {
				httpServer = get_server(user)
				aObject := factoryObjectA()

				aRecord, err := dataProcessor.CreateRecord(aObject.Name, map[string]interface{}{"name": "A record"}, auth.User{})

				Expect(err).To(BeNil())

				url := fmt.Sprintf("%s/data/%s/%s", appConfig.UrlPrefix, aObject.Name, aRecord.PkAsString())

				var request, _ = http.NewRequest("GET", url, nil)
				Expect(request.Response).To(BeNil())

				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("OK"))
			})
		})
	})

	Describe("'Data' scope tests", func() {
		BeforeEach(func() {
			user = &auth.User{
				Role: abac.JsonToObject(`{"id": "admin"}`),
				ABAC: map[string]interface{}{
					"_default_resolution": "deny",
					SERVICE_DOMAIN: map[string]interface{}{
						testObjName: map[string]interface{}{
							"data_GET": []interface{}{
								map[string]interface{}{
									"result": "allow",
									"rule": map[string]interface{}{
										"obj.owner_role": map[string]interface{}{"eq": "sbj.role.id"},
									},
								},
							},
							"data_PATCH": []interface{}{
								map[string]interface{}{
									"result": "allow",
									"rule": map[string]interface{}{
										"obj.owner_role": map[string]interface{}{"eq": "sbj.role.id"},
									},
								},
							},
							"data_DELETE": []interface{}{
								map[string]interface{}{
									"result": "allow",
									"rule": map[string]interface{}{
										"obj.owner_role": map[string]interface{}{"eq": "sbj.role.id"},
									},
								},
							},
						},
					},
				},
			}
		})

		JustBeforeEach(func() {
			httpServer = get_server(user)
		})

		Context("And an A record belongs to managers", func() {
			var err error
			var url string
			var aObject *meta.Meta
			var aRecord *record.Record

			JustBeforeEach(func() {
				aObject = factoryObjectA()
				aRecord, err = dataProcessor.CreateRecord(aObject.Name, map[string]interface{}{"name": "A record", "owner_role": "manager"}, auth.User{})
				Expect(err).To(BeNil())

				url = fmt.Sprintf("%s/data/%s/%s", appConfig.UrlPrefix, aObject.Name, aRecord.PkAsString())
			})

			It("Should return error when trying to retrieve a record of object A", func() {
				var request, _ = http.NewRequest("GET", url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("FAIL"))
			})

			It("Should return error when trying to update a record of object A", func() {
				updateData := map[string]interface{}{
					"name": "A updated name",
				}
				encodedMetaData, _ := json.Marshal(updateData)

				var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
				request.Header.Set("Content-Type", "application/json")
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("FAIL"))
			})

			It("Should return error when trying to delete a record of object A", func() {
				var request, _ = http.NewRequest("DELETE", url, nil)
				request.Header.Set("Content-Type", "application/json")
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("FAIL"))
			})

		})

		Context("And this user has the role 'admin'", func() {
			var err error
			var url string
			var aObject *meta.Meta
			var aRecord *record.Record

			JustBeforeEach(func() {
				aObject = factoryObjectA()
				aRecord, err = dataProcessor.CreateRecord(aObject.Name, map[string]interface{}{"name": "A record", "owner_role": "admin"}, auth.User{})
				Expect(err).To(BeNil())
				url = fmt.Sprintf("%s/data/%s/%s", appConfig.UrlPrefix, aObject.Name, aRecord.PkAsString())
			})

			It("Should return a record of object A", func() {
				var request, _ = http.NewRequest("GET", url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("OK"))
			})

			It("Should update a record of object A", func() {
				updateData := map[string]interface{}{
					"name": "A updated name",
				}
				encodedMetaData, _ := json.Marshal(updateData)

				var request, _ = http.NewRequest("PATCH", url, bytes.NewBuffer(encodedMetaData))
				request.Header.Set("Content-Type", "application/json")
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("OK"))
			})

			It("Should return error when trying to delete a record of object A", func() {
				var request, _ = http.NewRequest("DELETE", url, nil)
				request.Header.Set("Content-Type", "application/json")
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("OK"))
			})
		})

	})

	Describe("Deny result tests", func() {

		var obj_url, list_url string
		JustBeforeEach(func() {
			aObject := factoryObjectA()
			aRecord, err := dataProcessor.CreateRecord(aObject.Name, map[string]interface{}{"name": "A record", "owner_role": "manager"}, auth.User{})
			aRecord, err = dataProcessor.CreateRecord(aObject.Name, map[string]interface{}{"name": "Blue record", "owner_role": "user", "color": "blue"}, auth.User{})
			aRecord, err = dataProcessor.CreateRecord(aObject.Name, map[string]interface{}{"name": "Red record", "owner_role": "user"}, auth.User{})
			Expect(err).To(BeNil())

			list_url = fmt.Sprintf("%s/data/%s", appConfig.UrlPrefix, aObject.Name)
			obj_url = fmt.Sprintf("%s/data/%s/%s", appConfig.UrlPrefix, aObject.Name, aRecord.PkAsString())
		})

		Context("Sbj rules with default resoution set to deny globaly", func() {

			var abac_tree = map[string]interface{}{
				"_default_resolution": "deny",
				SERVICE_DOMAIN: map[string]interface{}{
					testObjName: map[string]interface{}{
						"data_GET": []interface{}{
							map[string]interface{}{
								"result": "allow",
								"rule": map[string]interface{}{
									"sbj.role.id": "admin",
								},
							}, map[string]interface{}{
								"result": "deny",
								"rule": map[string]interface{}{
									"sbj.role.id": "disabled",
								},
							}, map[string]interface{}{
								"result": "deny",
								"rule": map[string]interface{}{
									"sbj.role.id": "restricted",
									"obj.color":   "red",
								},
							},
						},
					},
				},
			}

			It("Must deny if rules not matched", func() {
				user = &auth.User{
					Role: abac.JsonToObject(`{"id": "test"}`), ABAC: abac_tree,
				}
				httpServer = get_server(user)

				var request, _ = http.NewRequest("GET", obj_url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("FAIL"))
			})

			It("Must allow if matched rule result is allow", func() {
				user = &auth.User{
					Role: abac.JsonToObject(`{"id": "admin"}`), ABAC: abac_tree,
				}
				httpServer = get_server(user)

				var request, _ = http.NewRequest("GET", obj_url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("OK"))
			})

			It("Must allow if no rules matched but domain result is overriden to allow", func() {
				abac_tree[SERVICE_DOMAIN].(map[string]interface{})["_default_resolution"] = "allow"
				user = &auth.User{
					Role: abac.JsonToObject(`{"id": "test"}`), ABAC: abac_tree,
				}
				httpServer = get_server(user)

				var request, _ = http.NewRequest("GET", obj_url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("OK"))
			})

			It("Must deny if matched rule result is deny", func() {
				abac_tree[SERVICE_DOMAIN].(map[string]interface{})["_default_resolution"] = "allow"
				user = &auth.User{
					Role: abac.JsonToObject(`{"id": "disabled"}`), ABAC: abac_tree,
				}
				httpServer = get_server(user)

				var request, _ = http.NewRequest("GET", obj_url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("FAIL"))
			})

			It("Must deny if only obj rules is set with deny resolution", func() {
				var abac_tree = map[string]interface{}{
					"_default_resolution": "allow",
					SERVICE_DOMAIN: map[string]interface{}{
						testObjName: map[string]interface{}{
							"data_GET": []interface{}{
								map[string]interface{}{
									"result": "deny",
									"rule": map[string]interface{}{
										"obj.color": "red",
									},
								},
							},
						},
					},
				}

				user = &auth.User{
					ABAC: abac_tree,
				}

				httpServer = get_server(user)

				var request, _ = http.NewRequest("GET", obj_url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)

				fmt.Fprintln(GinkgoWriter, body)
				Expect(body["status"].(string)).To(Equal("FAIL"))
			})
		})

		Context("Must invert rules and deny resolution", func() {
			var abac_tree = map[string]interface{}{
				"_default_resolution": "allow",
				SERVICE_DOMAIN: map[string]interface{}{
					testObjName: map[string]interface{}{
						"data_GET": []interface{}{
							map[string]interface{}{
								"result": "deny",
								"rule": map[string]interface{}{
									"sbj.role.id": "user",
									"obj.color":   "red",
								},
							},
						},
					},
				},
			}

			It("Must show only blue for users", func() {
				user = &auth.User{
					Role: abac.JsonToObject(`{"id": "user"}`), ABAC: abac_tree,
				}
				httpServer = get_server(user)

				var request, _ = http.NewRequest("GET", list_url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)

				Expect(body["status"].(string)).To(Equal("RESTRICTED"))
				data := body["data"].([]interface{})
				Expect(len(data)).To(Equal(1))
				Expect(data[0].(map[string]interface{})["color"].(string)).To(Equal("blue"))
			})
		})
	})
})
