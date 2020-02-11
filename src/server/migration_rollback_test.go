package server_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"server/object/meta"
	"server/pg"
	"utils"

	"server"

	"server/pg/migrations/managers"
	pg_transactions "server/pg/transactions"
	"server/transactions"

	"bytes"
	"encoding/json"
	"fmt"
	migrations_description "server/migrations/description"
)

var _ = Describe("Rollback migrations", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
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
		// drop history
		err := migrationManager.DropHistory()
		Expect(err).To(BeNil())
		//Flush meta/database
		err = metaStore.Flush()
		Expect(err).To(BeNil())
	}

	AfterEach(flushDb)

	Context("Having applied `create` migration for object A", func() {
		var aMetaDescription *meta.Meta
		var firstAppliedMigrationDescription *migrations_description.MigrationDescription

		BeforeEach(func() {
			//Create object A by applying a migration
			aMetaDescription = &meta.Meta{
				Name: "a",
				Key: "id",
				Fields: []*meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
				},
				Actions: nil,
				Cas: false,
			}

			firstAppliedMigrationDescription = &migrations_description.MigrationDescription{
				Id:        "1",
				ApplyTo:   "",
				DependsOn: nil,
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: aMetaDescription,
					},
				},
			}

			var err error
			aMetaDescription, err = migrationManager.Apply(firstAppliedMigrationDescription, true, false)
			Expect(err).To(BeNil())
		})

		Context("Having applied `addField` migration for object A", func() {
			var secondAppliedMigrationDescription *migrations_description.MigrationDescription

			BeforeEach(func() {
				field := meta.Field{
					Name:     "title",
					Type:     meta.FieldTypeString,
					Optional: false,
				}

				secondAppliedMigrationDescription = &migrations_description.MigrationDescription{
					Id:        "2",
					ApplyTo:   aMetaDescription.Name,
					DependsOn: []string{firstAppliedMigrationDescription.Id},
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				var err error
				aMetaDescription, err = migrationManager.Apply(secondAppliedMigrationDescription, true, false)
				Expect(err).To(BeNil())
			})

			Context("Having applied `UpdateField` migration for object A", func() {
				var thirdAppliedMigrationDescription *migrations_description.MigrationDescription

				BeforeEach(func() {
					//Create object A by applying a migration
					field := meta.Field{
						Name:     "new_title",
						Type:     meta.FieldTypeString,
						Optional: false,
					}

					thirdAppliedMigrationDescription = &migrations_description.MigrationDescription{
						Id:        "3",
						ApplyTo:   aMetaDescription.Name,
						DependsOn: []string{secondAppliedMigrationDescription.Id},
						Operations: [] migrations_description.MigrationOperationDescription{
							{
								Type:  migrations_description.UpdateFieldOperation,
								Field: &migrations_description.MigrationFieldDescription{Field: field, PreviousName: "title"},
							},
						},
					}

					var err error
					aMetaDescription, err = migrationManager.Apply(thirdAppliedMigrationDescription, true, false)
					Expect(err).To(BeNil())
				})

				It("It can rollback object`s state up to the first migration state", func() {
					url := fmt.Sprintf("%s/migrations/rollback", appConfig.UrlPrefix)

					data := map[string]interface{}{
						"migrationId": firstAppliedMigrationDescription.Id,
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

					records, err := migrationManager.GetPrecedingMigrationsForObject(aMetaDescription.Name)
					Expect(err).To(BeNil())

					Expect(records).To(HaveLen(1))
					Expect(records[0].Data["migration_id"]).To(Equal(firstAppliedMigrationDescription.Id))
				})
			})
		})
	})
})
