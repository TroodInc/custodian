package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"custodian/server/object"
	"custodian/server/pg"
	"custodian/server/transactions/file_transaction"
	"custodian/utils"

	"encoding/json"
	"fmt"
	"custodian/server"
	migrations_description "custodian/server/migrations/description"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/pg/migrations/managers"
	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions"
	"custodian/server/pg_meta"
)

var _ = Describe("Migrations` listing", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg_meta.NewPgMetaDescriptionSyncer(dbTransactionManager)

	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	migrationManager := managers.NewMigrationManager(
		metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
	)

	BeforeEach(func() {
		//setup server
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionUrl).Setup(appConfig)
		recorder = httptest.NewRecorder()
	})

	flushDb := func() {
		//Flush meta/database
		// drop history
		err := migrationManager.DropHistory()
		Expect(err).To(BeNil())
		err = metaStore.Flush()
		Expect(err).To(BeNil())
	}

	BeforeEach(flushDb)
	AfterEach(flushDb)

	Context("Having an applied migration", func() {
		var metaDescription *description.MetaDescription
		var migrationDescription *migrations_description.MigrationDescription
		BeforeEach(func() {
			metaDescription = object.GetBaseMetaData(utils.RandomString(8))

			migrationDescription = &migrations_description.MigrationDescription{
				Id:        "some-unique-id",
				ApplyTo:   "",
				DependsOn: nil,
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: metaDescription,
					},
				},
			}

			_, err := migrationManager.Apply(migrationDescription,true, false)

			migrationDescription2 := &migrations_description.MigrationDescription{
				Id:        "with-depends",
				ApplyTo:   metaDescription.Name,
				DependsOn: []string{"some-unique-id"},
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:   migrations_description.AddActionOperation,
						Action: &migrations_description.MigrationActionDescription{
							Action: description.Action{
								Method:          description.MethodUpdate,
								Protocol:        description.REST,
								Args:            []string{"http://localhost"},
								ActiveIfNotRoot: false,
								IncludeValues:   nil,
								Name:            "Testaction",
							},
						},
					},
				},
			}

			_, err = migrationManager.Apply(migrationDescription2,true, false)
			Expect(err).To(BeNil())
		})

		It("Can list applied migrations", func() {

			url := fmt.Sprintf("%s/migrations", appConfig.UrlPrefix)

			filter := "eq(applyTo," + metaDescription.Name + ")"

			var request, _ = http.NewRequest("GET", url+"?q="+filter, nil)
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))

			//ensure applied migration was returned
			data := body["data"].([]interface{})
			Expect(data).To(HaveLen(2))
			first := data[0].(map[string]interface{})
			Expect(first["id"]).To(Equal(migrationDescription.Id))
		})

		It("Can detail applied migrations", func() {

			url := fmt.Sprintf("%s/migrations/with-depends", appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))
			//ensure applied migration was returned
			data := body["data"].(map[string]interface{})
			Expect(data).NotTo(BeNil())
			Expect(data["id"]).To(Equal("with-depends"))
			Expect(data["dependsOn"]).To(HaveLen(1))
			Expect(data["dependsOn"]).To(ContainElement("some-unique-id"))
		})
	})

})
