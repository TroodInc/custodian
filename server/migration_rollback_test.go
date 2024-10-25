package server_test

import (
	"custodian/server/object"
	"custodian/utils"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"custodian/server"
	"custodian/server/object/description"
	"custodian/server/object/migrations/managers"

	"bytes"
	migrations_description "custodian/server/migrations/description"
	"encoding/json"
	"fmt"
)

var _ = Describe("Rollback migrations", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)
	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache(), db)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)

	migrationManager := managers.NewMigrationManager(
		metaDescriptionSyncer, dbTransactionManager, db,
	)

	BeforeEach(func() {
		//setup server
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionUrl).Setup(appConfig)
		recorder = httptest.NewRecorder()
	})

	flushDb := func() {
		// drop history
		_, err := db.Exec(managers.TRUNCATE_MIGRATION_HISTORY_TABLE)
		Expect(err).To(BeNil())
		//Flush meta/database
		err = metaStore.Flush()
		Expect(err).To(BeNil())
	}

	AfterEach(flushDb)

	Context("Having applied `create` migration for object A", func() {
		var aMetaDescription *description.MetaDescription
		var firstAppliedMigrationDescription *migrations_description.MigrationDescription

		BeforeEach(func() {
			//Create object A by applying a migration
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())
			testObjAName := utils.RandomString(8)

			aMetaDescription = description.NewMetaDescription(
				testObjAName,
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

			firstAppliedMigrationDescription = &migrations_description.MigrationDescription{
				Id:        "1",
				ApplyTo:   "",
				DependsOn: nil,
				Operations: []migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: aMetaDescription,
					},
				},
			}

			aMetaDescription, err = migrationManager.Apply(firstAppliedMigrationDescription, true, false)
			Expect(err).To(BeNil())

			globalTransaction.Commit()
		})

		Context("Having applied `addField` migration for object A", func() {
			var secondAppliedMigrationDescription *migrations_description.MigrationDescription

			BeforeEach(func() {
				//Create object A by applying a migration
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				field := description.Field{
					Name:     "title",
					Type:     description.FieldTypeString,
					Optional: false,
				}

				secondAppliedMigrationDescription = &migrations_description.MigrationDescription{
					Id:        "2",
					ApplyTo:   aMetaDescription.Name,
					DependsOn: []string{firstAppliedMigrationDescription.Id},
					Operations: []migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				aMetaDescription, err = migrationManager.Apply(secondAppliedMigrationDescription, true, false)
				Expect(err).To(BeNil())

				globalTransaction.Commit()
			})

			Context("Having applied `UpdateField` migration for object A", func() {
				var thirdAppliedMigrationDescription *migrations_description.MigrationDescription

				BeforeEach(func() {
					//Create object A by applying a migration
					globalTransaction, err := dbTransactionManager.BeginTransaction()
					Expect(err).To(BeNil())

					field := description.Field{
						Name:     "new_title",
						Type:     description.FieldTypeString,
						Optional: false,
					}

					thirdAppliedMigrationDescription = &migrations_description.MigrationDescription{
						Id:        "3",
						ApplyTo:   aMetaDescription.Name,
						DependsOn: []string{secondAppliedMigrationDescription.Id},
						Operations: []migrations_description.MigrationOperationDescription{
							{
								Type:  migrations_description.UpdateFieldOperation,
								Field: &migrations_description.MigrationFieldDescription{Field: field, PreviousName: "title"},
							},
						},
					}

					aMetaDescription, err = migrationManager.Apply(thirdAppliedMigrationDescription, true, false)
					Expect(err).To(BeNil())

					globalTransaction.Commit()
				})

				It("It can rollback object`s state up to the first migration state", func() {
					url := fmt.Sprintf("%s/migrations/%s/rollback", appConfig.UrlPrefix, firstAppliedMigrationDescription.Id)

					encodedData, _ := json.Marshal(nil)

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
					Expect(records[0].Data["id"]).To(Equal(firstAppliedMigrationDescription.Id))
				})
			})
		})
	})
})
