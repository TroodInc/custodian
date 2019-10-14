package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"server/pg"
	"server/transactions/file_transaction"
	"utils"

	"encoding/json"
	"fmt"
	"server"
	migrations_description "server/migrations/description"
	"server/object/description"
	"server/object/meta"
	"server/pg/migrations/managers"
	pg_transactions "server/pg/transactions"
	"server/transactions"
)

var _ = Describe("Migrations` listing", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)
	migrationManager := managers.NewMigrationManager(
		metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
	)

	BeforeEach(func() {
		//setup server
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionOptions).Setup(appConfig)
		recorder = httptest.NewRecorder()
	})

	flushDb := func() {
		//Flush meta/database
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		err = metaStore.Flush()
		Expect(err).To(BeNil())
		// drop history
		err = managers.NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
		).DropHistory(globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	}

	BeforeEach(flushDb)
	AfterEach(flushDb)

	Context("Having an applied migration", func() {
		var metaDescription *description.MetaDescription
		var migrationDescription *migrations_description.MigrationDescription
		BeforeEach(func() {

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			metaDescription = description.NewMetaDescription(
				"b",
				"id",
				[]description.Field{
					{
						Name: "id",
						Type: description.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
				},
				nil,
				false,
			)

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

			_, err = migrationManager.Apply(migrationDescription, globalTransaction, true)
			Expect(err).To(BeNil())
			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())

		})
		It("Can list applied migrations", func() {

			url := fmt.Sprintf("%s/migrations", appConfig.UrlPrefix)

			filter := "eq(object," + metaDescription.Name + ")"

			var request, _ = http.NewRequest("GET", url+"?q="+filter, nil)
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))
			//ensure applied migration was returned
			Expect(body["data"].([]interface{})).To(HaveLen(1))
			first := body["data"].([]interface{})[0].(map[string]interface{})
			Expect(first["Data"].(map[string]interface{})["migration_id"]).To(Equal(migrationDescription.Id))
		})

		It("Can detail applied migrations", func() {

			url := fmt.Sprintf("%s/migrations/description/"+migrationDescription.Id, appConfig.UrlPrefix)

			var request, _ = http.NewRequest("GET", url, nil)
			httpServer.Handler.ServeHTTP(recorder, request)

			responseBody := recorder.Body.String()
			var body map[string]interface{}
			json.Unmarshal([]byte(responseBody), &body)

			//check response status
			Expect(body["status"]).To(Equal("OK"))
			//ensure applied migration was returned
			Expect(body["data"]).NotTo(BeNil())
			Expect(body["data"].(map[string]interface{})["id"]).To(Equal(migrationDescription.Id))
		})
	})

})
