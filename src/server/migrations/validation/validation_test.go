package validation_test

import (
	"encoding/json"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"server/errors"
	"server/pg"
	"server/transactions/file_transaction"
	"utils"

	"server"
	"server/migrations"
	migrations_description "server/migrations/description"
	"server/migrations/validation"
	"server/object/description"
	"server/object/meta"
	"server/pg/migrations/managers"
	pg_transactions "server/pg/transactions"
	"server/transactions"
)

var _ = Describe("Migration Validation Service", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	migrationDBDescriptionSyncer := pg.NewDbMetaDescriptionSyncer(dbTransactionManager)
	migrationStore := meta.NewStore(migrationDBDescriptionSyncer, syncer, globalTransactionManager)
	migrationManager := managers.NewMigrationManager(
		metaStore, migrationStore, dataManager, globalTransactionManager,
	)

	migrationValidationService := validation.NewMigrationValidationService(migrationManager)

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

	It("It does`nt return error if there are no parents actually and no parents are specified in migration`s description", func() {
		metaDescription := description.GetBasicMetaDescription("random")
		migrationDescription := migrations_description.GetObjectCreationMigration("random", "", nil, metaDescription)

		err := migrationValidationService.Validate(migrationDescription)
		Expect(err).To(BeNil())
	})

	Context("Having applied `create` migration for object A", func() {
		var aMetaDescription *description.MetaDescription
		var firstAppliedMigrationDescription *migrations_description.MigrationDescription

		BeforeEach(func() {
			//Create object A by applying a migration
			aMetaDescription = description.GetBasicMetaDescription("random")
			firstAppliedMigrationDescription = migrations_description.GetObjectCreationMigration("random", "", nil, aMetaDescription)

			var err error
			aMetaDescription, err = migrationManager.Apply(firstAppliedMigrationDescription, true, false)
			Expect(err).To(BeNil())
		})

		It("It returns an error if there is an already applied migration but it is not specified in migration`s description", func() {
			migrationDescription := migrations_description.GetObjectCreationMigration("random", "", nil, aMetaDescription)

			err := migrationValidationService.Validate(migrationDescription)
			Expect(err).NotTo(BeNil())
			Expect(err.(*errors.ServerError).Code).To(Equal(migrations.MigrationIsNotActual))
		})

		It("It does`nt return error if there is an already applied migration and its id is specified in migration", func() {
			field := description.Field{
				Name:     "title",
				Type:     description.FieldTypeString,
				Optional: false,
			}

			migrationDescription := migrations_description.GetFieldCreationMigration(
				"random", aMetaDescription.Name, []string{firstAppliedMigrationDescription.Id}, field,
			)

			err := migrationValidationService.Validate(migrationDescription)
			Expect(err).To(BeNil())
		})

		Context("Having a second applied migration for the same object", func() {
			var secondAppliedMigrationDescription *migrations_description.MigrationDescription

			BeforeEach(func() {
				//Add a field to the object A
				field := description.Field{
					Name:     "title",
					Type:     description.FieldTypeString,
					Optional: false,
				}

				secondAppliedMigrationDescription = migrations_description.GetFieldCreationMigration(
					"random", aMetaDescription.Name, []string{firstAppliedMigrationDescription.Id}, field,
				)

				var err error
				aMetaDescription, err = migrationManager.Apply(secondAppliedMigrationDescription,true, false)
				Expect(err).To(BeNil())
			})

			It("It returns an error if a migration contains an `object` operation and supposed to be applied as a sibling", func() {
				newMetaDescription := description.GetBasicMetaDescription("random")

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "3",
					ApplyTo:   aMetaDescription.Name,
					DependsOn: []string{firstAppliedMigrationDescription.Id},
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:            migrations_description.RenameObjectOperation,
							MetaDescription: newMetaDescription,
						},
					},
				}

				err := migrationValidationService.Validate(migrationDescription)
				Expect(err).NotTo(BeNil())
				Expect(err.(*errors.ServerError).Code).To(Equal(migrations.MigrationIsNotCompatibleWithSiblings))
			})

			It("It returns an error if a migration contains an `field` operation on the same field as the applied sibling has", func() {
				field := description.Field{
					Name:     "title",
					Type:     description.FieldTypeString,
					Optional: false,
				}

				migrationDescription := migrations_description.GetFieldCreationMigration(
					"random", aMetaDescription.Name, []string{firstAppliedMigrationDescription.Id}, field,
				)

				err := migrationValidationService.Validate(migrationDescription)
				Expect(err).NotTo(BeNil())
				Expect(err.(*errors.ServerError).Code).To(Equal(migrations.MigrationIsNotCompatibleWithSiblings))
			})

			It("It returns an error if a migration contains an `field` `rename` operation", func() {
				field := description.Field{
					Name:     "updated_title",
					Type:     description.FieldTypeString,
					Optional: false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "3",
					ApplyTo:   aMetaDescription.Name,
					DependsOn: []string{firstAppliedMigrationDescription.Id},
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.UpdateFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field, PreviousName: "title"},
						},
					},
				}

				err := migrationValidationService.Validate(migrationDescription)
				Expect(err).NotTo(BeNil())
				Expect(err.(*errors.ServerError).Code).To(Equal(migrations.MigrationIsNotCompatibleWithSiblings))
			})

			It("Can apply a migration which is a sibling to the latest applied", func() {
				//Add a field to the object A
				field := description.Field{
					Name:     "content",
					Type:     description.FieldTypeString,
					Optional: false,
				}

				migrationDescription := migrations_description.GetFieldCreationMigration(
					"random", aMetaDescription.Name, []string{firstAppliedMigrationDescription.Id}, field,
				)

				var err error
				aMetaDescription, err = migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				migrationRecords, err := migrationManager.GetPrecedingMigrationsForObject(aMetaDescription.Name)
				Expect(err).To(BeNil())

				Expect(migrationRecords).To(HaveLen(2))
				Expect(migrationRecords[0].Data["migration_id"]).To(Equal(secondAppliedMigrationDescription.Id))
				Expect(migrationRecords[1].Data["migration_id"]).To(Equal(migrationDescription.Id))

			})

			Context("Having a third applied migration for the same object which actually is a sibling of the second one", func() {
				var thirdAppliedMigrationDescription *migrations_description.MigrationDescription
				BeforeEach(func() {
					//Add a field to the object A
					field := description.Field{
						Name:     "content",
						Type:     description.FieldTypeString,
						Optional: false,
					}

					thirdAppliedMigrationDescription = migrations_description.GetFieldCreationMigration(
						"random", aMetaDescription.Name, []string{firstAppliedMigrationDescription.Id}, field,
					)

					var err error
					aMetaDescription, err = migrationManager.Apply(thirdAppliedMigrationDescription, true, false)
					Expect(err).To(BeNil())
				})

				It("Returns an error if a migration has an outdated list of a direct parents", func() {
					//Add a field to the object A
					field := description.Field{
						Name:     "publish_date",
						Type:     description.FieldTypeDate,
						Optional: false,
					}

					migrationDescription := migrations_description.GetFieldCreationMigration(
						"random", aMetaDescription.Name, []string{secondAppliedMigrationDescription.Id}, field,
					)

					err := migrationValidationService.Validate(migrationDescription)
					Expect(err).NotTo(BeNil())
					Expect(err.(*errors.ServerError).Code).To(Equal(migrations.MigrationErrorParentsChanged))
				})

				Context("Having a fourth applied migration for the same object which is a direct children to the second and third", func() {
					var fourthAppliedMigrationDescription *migrations_description.MigrationDescription
					BeforeEach(func() {
						field := description.Field{
							Name:     "publish_date",
							Type:     description.FieldTypeDate,
							Optional: false,
						}

						fourthAppliedMigrationDescription = migrations_description.GetFieldCreationMigration(
							"random", aMetaDescription.Name, []string{secondAppliedMigrationDescription.Id, thirdAppliedMigrationDescription.Id}, field,
						)

						var err error
						aMetaDescription, err = migrationManager.Apply(fourthAppliedMigrationDescription, true, false)
						Expect(err).To(BeNil())
					})

					XIt("Returns an error if a migration is not actual and its siblings already have children", func() {
						field := description.Field{
							Name:     "author",
							Type:     description.FieldTypeObject,
							LinkMeta: "staff",
							Optional: false,
						}

						precd, _ := migrationManager.List("eq")
						for _, p := range precd {
							ss, _ := json.Marshal(p.Data)
							fmt.Fprintln(GinkgoWriter, fmt.Sprintf("%s", ss))
						}
						fmt.Fprintln(GinkgoWriter, aMetaDescription.Name)
						fmt.Fprintln(GinkgoWriter, secondAppliedMigrationDescription.Id)

						migrationDescription := migrations_description.GetFieldCreationMigration(
							"random", aMetaDescription.Name, []string{secondAppliedMigrationDescription.Id}, field,
						)

						err := migrationValidationService.Validate(migrationDescription)
						Expect(err).NotTo(BeNil())
						Expect(err.(*errors.ServerError).Code).To(Equal(migrations.MigrationIsNotActual))
					})
				})
			})
		})
	})
})
