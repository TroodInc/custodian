package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"server/pg"
	"utils"
	"server/transactions/file_transaction"

	"server/object/meta"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server"
	"server/object/description"
	"server/pg/migrations/managers"

	migrations_description "server/migrations/description"
	"encoding/json"
	"fmt"
	"bytes"
)

var _ = Describe("Rollback migrations", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)

	migrationDBDescriptionSyncer := pg.NewDbMetaDescriptionSyncer(dbTransactionManager)
	migrationStore := meta.NewStore(migrationDBDescriptionSyncer, syncer, globalTransactionManager)
	migrationManager := managers.NewMigrationManager(
		metaStore, migrationStore, dataManager, globalTransactionManager,
	)

	BeforeEach(func() {
		//setup server
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionUrl).Setup(appConfig)
		recorder = httptest.NewRecorder()

		// drop history
		err := migrationManager.DropHistory()
		Expect(err).To(BeNil())
		//Flush meta/database
		err = metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("It can rollback object`s state up to the first migration state", func() {
		By("Having applied `create` migration for object A")
		//Create object by applying a migration
		migrationManager.Apply(&migrations_description.MigrationDescription{
			Id:        "1",
			ApplyTo:   "",
			DependsOn: nil,
			Operations: [] migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.CreateObjectOperation,
					MetaDescription: description.NewMetaDescription(
						"rollback_test_object",
						"id",
						[]description.Field{
							{Name: "id", Type: description.FieldTypeNumber, Def: map[string]interface{}{"func": "nextval"}},
						},
						nil,
						false,
					),
				},
			},
		}, false, true)

		By("Having applied `addField` migration for object A")
		//Create object A by applying a migration
		migrationManager.Apply(&migrations_description.MigrationDescription{
			Id:        "2",
			ApplyTo:   "rollback_test_object",
			DependsOn: []string{"2"},
			Operations: [] migrations_description.MigrationOperationDescription{
				{
					Type:  migrations_description.AddFieldOperation,
					Field: &migrations_description.MigrationFieldDescription{
						Field: description.Field{Name: "title", Type: description.FieldTypeString, Optional: false},
					},
				},
			},
		}, false, true)

		By("Having applied `UpdateField` migration for object A")
		migrationManager.Apply(&migrations_description.MigrationDescription{
			Id:        "3",
			ApplyTo:   "rollback_test_object",
			DependsOn: []string{"2"},
			Operations: [] migrations_description.MigrationOperationDescription{
				{
					Type:  migrations_description.UpdateFieldOperation,
					Field: &migrations_description.MigrationFieldDescription{
						Field: description.Field{Name: "new_title", Type: description.FieldTypeString, Optional: false},
						PreviousName: "title"},
				},
			},
		}, false, true)

		url := fmt.Sprintf("%s/migrations/rollback", appConfig.UrlPrefix)

		data := map[string]interface{}{
			"migrationId": "1",
		}
		encodedData, _ := json.Marshal(data)

		var request, _ = http.NewRequest("POST", url, bytes.NewBuffer(encodedData))
		request.Header.Set("Content-Type", "application/json")
		httpServer.Handler.ServeHTTP(recorder, request)
		responseBody := recorder.Body.String()
		var body map[string]interface{}
		json.Unmarshal([]byte(responseBody), &body)

		//check response status
		Expect(body["status"]).To(Equal("OK"))
		//ensure applied migration was returned
		Expect(body["data"]).NotTo(BeNil())

		//ensure rolled back migrations were removed from history
		records, err := migrationManager.GetPrecedingMigrationsForObject("rollback_test_object")
		Expect(err).To(BeNil())

		Expect(records).To(HaveLen(1))
		Expect(records[0].Data["migration_id"]).To(Equal("1"))
	})
})
