package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"server/pg"

	"utils"
	"server/object/meta"
	"server/transactions/file_transaction"

	pg_transactions "server/pg/transactions"
	"server/transactions"
	"net/http/httptest"
	"server"
	"server/auth"
	"server/data"
	"server/object/description"
	"encoding/json"
	"fmt"
	"os"
	"bytes"
	"server/data/record"
)

const SERVICE_DOMAIN = "custodian"

var _ = Describe("ABAC rules handling", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	var httpServer *http.Server
	var custodianServer *server.CustodianServer
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	var globalTransaction *transactions.GlobalTransaction

	var user *auth.User

	flushDb := func() {
		var err error
		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
	}

	factoryObjectA := func(globalTransaction *transactions.GlobalTransaction) *meta.Meta {
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
					Name:     "owner_role",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	JustBeforeEach(func() {
		flushDb()

		os.Setenv("SERVICE_DOMAIN", SERVICE_DOMAIN)

		custodianServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionOptions)

		Expect(user).NotTo(BeNil())
		custodianServer.SetAuthenticator(auth.NewMockAuthenticator(*user))

		httpServer = custodianServer.Setup(false)
		recorder = httptest.NewRecorder()

	})

	AfterEach(flushDb)

	Describe("'Subject' scope tests", func() {
		BeforeEach(func() {
			user = &auth.User{
				Role: "admin",
				ABAC: map[string]interface{}{
					SERVICE_DOMAIN: map[string]interface{}{
						"a": map[string]interface{}{
							"data_single_GET": []interface{}{
								map[string]interface{}{
									"result": "allow",
									"rule": map[string]interface{}{
										"sbj.role": map[string]interface{}{"eq": "admin"},
									},
								},
							},
						},
					},
				},
			}
		})
		Context("And this user has the role 'manager'", func() {
			BeforeEach(func() {
				user.Role = "manager"
			})
			It("Should return error when trying to retrieve a record of object A", func() {
				aObject := factoryObjectA(globalTransaction)
				aRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aObject.Name, map[string]interface{}{"name": "A record"}, auth.User{})

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())

				url := fmt.Sprintf("%s/data/single/%s/%s", appConfig.UrlPrefix, aObject.Name, aRecord.PkAsString())

				var request, _ = http.NewRequest("GET", url, nil)
				httpServer.Handler.ServeHTTP(recorder, request)
				responseBody := recorder.Body.String()

				var body map[string]interface{}
				json.Unmarshal([]byte(responseBody), &body)
				Expect(body["status"].(string)).To(Equal("FAIL"))
			})
		})

		Context("And this user has the role 'admin'", func() {
			BeforeEach(func() {
				user.Role = "admin"
			})
			It("Should return a record of object A", func() {
				aObject := factoryObjectA(globalTransaction)
				aRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aObject.Name, map[string]interface{}{"name": "A record"}, auth.User{})

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())

				url := fmt.Sprintf("%s/data/single/%s/%s", appConfig.UrlPrefix, aObject.Name, aRecord.PkAsString())

				var request, _ = http.NewRequest("GET", url, nil)
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
				Role: "admin",
				ABAC: map[string]interface{}{
					SERVICE_DOMAIN: map[string]interface{}{
						"a": map[string]interface{}{
							"data_single_GET": []interface{}{
								map[string]interface{}{
									"result": "allow",
									"rule": map[string]interface{}{
										"obj.owner_role": map[string]interface{}{"eq": "sbj.role"},
									},
								},
							},
							"data_single_POST": []interface{}{
								map[string]interface{}{
									"result": "allow",
									"rule": map[string]interface{}{
										"obj.owner_role": map[string]interface{}{"eq": "sbj.role"},
									},
								},
							},
							"data_single_DELETE": []interface{}{
								map[string]interface{}{
									"result": "allow",
									"rule": map[string]interface{}{
										"obj.owner_role": map[string]interface{}{"eq": "sbj.role"},
									},
								},
							},
						},
					},
				},
			}
		})

		Context("And an A record belongs to managers", func() {
			var err error
			var url string
			var aObject *meta.Meta
			var aRecord *record.Record

			JustBeforeEach(func() {
				aObject = factoryObjectA(globalTransaction)
				aRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, aObject.Name, map[string]interface{}{"name": "A record", "owner_role": "manager"}, auth.User{})
				Expect(err).To(BeNil())
				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())

				url = fmt.Sprintf("%s/data/single/%s/%s", appConfig.UrlPrefix, aObject.Name, aRecord.PkAsString())
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

				var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
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
				aObject = factoryObjectA(globalTransaction)
				aRecord, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, aObject.Name, map[string]interface{}{"name": "A record", "owner_role": "admin"}, auth.User{})
				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
				url = fmt.Sprintf("%s/data/single/%s/%s", appConfig.UrlPrefix, aObject.Name, aRecord.PkAsString())
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

				var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedMetaData))
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
})
