package validation_test

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
	"server/pg/migrations/managers"
	"server/migrations/validation"
	"server/object/description"
	migrations_description "server/migrations/description"
	"server/migrations"
)

var _ = Describe("Migration Validation Service", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer)

	var httpServer *http.Server
	var recorder *httptest.ResponseRecorder

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	migrationManager := managers.NewMigrationManager(metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath)

	migrationValidationService := validation.NewMigrationValidationService(migrationManager, appConfig.MigrationStoragePath)

	BeforeEach(func() {
		//setup server
		httpServer = server.New("localhost", "8081", appConfig.UrlPrefix, appConfig.DbConnectionOptions).Setup(appConfig)
		recorder = httptest.NewRecorder()
	})

	flushDb := func() {
		//Flush meta/database
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		err = metaStore.Flush(globalTransaction)
		Expect(err).To(BeNil())
		// drop history
		err = managers.NewMigrationManager(metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath).DropHistory(globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	}

	BeforeEach(flushDb)
	AfterEach(flushDb)

	It("It does`nt return error if there are no parents actually and no parents are specified in migration`s description", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		metaDescription := description.NewMetaDescription(
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
				{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				},
			},
			nil,
			false,
		)

		migrationDescription := &migrations_description.MigrationDescription{
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

		err = migrationValidationService.Validate(migrationDescription, globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	Context("Having applied `create` migration for object A", func() {
		var aMetaDescription *description.MetaDescription
		var firstAppliedMigrationDescription *migrations_description.MigrationDescription

		BeforeEach(func() {
			//Create object A by applying a migration
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			aMetaDescription = description.NewMetaDescription(
				"a",
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
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: aMetaDescription,
					},
				},
			}

			aMetaDescription, err = migrationManager.Run(firstAppliedMigrationDescription, globalTransaction, true)
			Expect(err).To(BeNil())

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("It returns an error if there is an already applied migration but it is not specified in migration`s description", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			metaDescription := description.NewMetaDescription(
				"a",
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

			migrationDescription := &migrations_description.MigrationDescription{
				Id:        "2",
				ApplyTo:   "",
				DependsOn: nil,
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: metaDescription,
					},
				},
			}

			err = migrationValidationService.Validate(migrationDescription, globalTransaction.DbTransaction)
			Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())
			Expect(err).NotTo(BeNil())
			Expect(err.(*migrations.MigrationError).Code()).To(Equal(migrations.MigrationIsNotActual))
		})

		It("It does`nt return error if there is an already applied migration and its id is specified in migration", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			field := description.Field{
				Name:     "title",
				Type:     description.FieldTypeString,
				Optional: false,
			}

			migrationDescription := &migrations_description.MigrationDescription{
				Id:        "3",
				ApplyTo:   aMetaDescription.Name,
				DependsOn: []string{firstAppliedMigrationDescription.Id},
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:  migrations_description.AddFieldOperation,
						Field: &migrations_description.MigrationFieldDescription{Field: field},
					},
				},
			}

			err = migrationValidationService.Validate(migrationDescription, globalTransaction.DbTransaction)
			Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())
			Expect(err).To(BeNil())
		})

		Context("Having a second applied migration for the same object", func() {
			var secondAppliedMigrationDescription *migrations_description.MigrationDescription

			BeforeEach(func() {
				//Add a field to the object A
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
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
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				aMetaDescription, err = migrationManager.Run(secondAppliedMigrationDescription, globalTransaction, true)
				Expect(err).To(BeNil())

				globalTransactionManager.CommitTransaction(globalTransaction)
			})

			It("It returns an error if a migration contains an `object` operation and supposed to be applied as a sibling", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				aMetaDescription = description.NewMetaDescription(
					"a",
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

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "3",
					ApplyTo:   "a",
					DependsOn: []string{firstAppliedMigrationDescription.Id},
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:            migrations_description.RenameObjectOperation,
							MetaDescription: aMetaDescription,
						},
					},
				}

				err = migrationValidationService.Validate(migrationDescription, globalTransaction.DbTransaction)
				Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())
				Expect(err).NotTo(BeNil())
				Expect(err.(*migrations.MigrationError).Code()).To(Equal(migrations.MigrationIsNotCompatibleWithSiblings))
			})

			It("It returns an error if a migration contains an `field` operation on the same field as the applied sibling has", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				field := description.Field{
					Name:     "title",
					Type:     description.FieldTypeString,
					Optional: false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "3",
					ApplyTo:   aMetaDescription.Name,
					DependsOn: []string{firstAppliedMigrationDescription.Id},
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				err = migrationValidationService.Validate(migrationDescription, globalTransaction.DbTransaction)
				Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())
				Expect(err).NotTo(BeNil())
				Expect(err.(*migrations.MigrationError).Code()).To(Equal(migrations.MigrationIsNotCompatibleWithSiblings))
			})

			It("It returns an error if a migration contains an `field` `rename` operation", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

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

				err = migrationValidationService.Validate(migrationDescription, globalTransaction.DbTransaction)
				Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())
				Expect(err).NotTo(BeNil())
				Expect(err.(*migrations.MigrationError).Code()).To(Equal(migrations.MigrationIsNotCompatibleWithSiblings))
			})

			It("Can apply a migration which is a sibling to the latest applied", func() {
				//Add a field to the object A
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				field := description.Field{
					Name:     "content",
					Type:     description.FieldTypeString,
					Optional: false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "3",
					ApplyTo:   aMetaDescription.Name,
					DependsOn: []string{firstAppliedMigrationDescription.Id},
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				aMetaDescription, err = migrationManager.Run(migrationDescription, globalTransaction, true)
				Expect(err).To(BeNil())

				migrationRecords, err := migrationManager.GetPrecedingMigrationsForObject(aMetaDescription.Name, globalTransaction.DbTransaction)
				globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationRecords).To(HaveLen(2))
				Expect(migrationRecords[0].Data["migration_id"]).To(Equal("2"))
				Expect(migrationRecords[1].Data["migration_id"]).To(Equal("3"))

			})

			Context("Having a third applied migration for the same object which actually is a sibling of the second one", func() {
				var thirdAppliedMigrationDescription *migrations_description.MigrationDescription
				BeforeEach(func() {
					//Add a field to the object A
					globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
					Expect(err).To(BeNil())

					field := description.Field{
						Name:     "content",
						Type:     description.FieldTypeString,
						Optional: false,
					}

					thirdAppliedMigrationDescription = &migrations_description.MigrationDescription{
						Id:        "3",
						ApplyTo:   aMetaDescription.Name,
						DependsOn: []string{firstAppliedMigrationDescription.Id},
						Operations: [] migrations_description.MigrationOperationDescription{
							{
								Type:  migrations_description.AddFieldOperation,
								Field: &migrations_description.MigrationFieldDescription{Field: field},
							},
						},
					}

					aMetaDescription, err = migrationManager.Run(thirdAppliedMigrationDescription, globalTransaction, true)
					Expect(err).To(BeNil())

					globalTransactionManager.CommitTransaction(globalTransaction)
				})

				It("Returns an error if a migration has an outdated list of a direct parents", func() {
					//Add a field to the object A
					globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
					Expect(err).To(BeNil())

					field := description.Field{
						Name:     "publish_date",
						Type:     description.FieldTypeDate,
						Optional: false,
					}

					migrationDescription := &migrations_description.MigrationDescription{
						Id:        "5",
						ApplyTo:   aMetaDescription.Name,
						DependsOn: []string{secondAppliedMigrationDescription.Id},
						Operations: [] migrations_description.MigrationOperationDescription{
							{
								Type:  migrations_description.AddFieldOperation,
								Field: &migrations_description.MigrationFieldDescription{Field: field},
							},
						},
					}

					err = migrationValidationService.Validate(migrationDescription, globalTransaction.DbTransaction)
					Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())
					Expect(err).NotTo(BeNil())
					Expect(err.(*migrations.MigrationError).Code()).To(Equal(migrations.MigrationErrorParentsChanged))
				})

				Context("Having a fourth applied migration for the same object which is a direct children to the second and third", func() {
					var fourthAppliedMigrationDescription *migrations_description.MigrationDescription
					BeforeEach(func() {
						globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
						Expect(err).To(BeNil())

						field := description.Field{
							Name:     "publish_date",
							Type:     description.FieldTypeDate,
							Optional: false,
						}

						fourthAppliedMigrationDescription = &migrations_description.MigrationDescription{
							Id:        "4",
							ApplyTo:   aMetaDescription.Name,
							DependsOn: []string{secondAppliedMigrationDescription.Id, thirdAppliedMigrationDescription.Id},
							Operations: [] migrations_description.MigrationOperationDescription{
								{
									Type:  migrations_description.AddFieldOperation,
									Field: &migrations_description.MigrationFieldDescription{Field: field},
								},
							},
						}

						aMetaDescription, err = migrationManager.Run(fourthAppliedMigrationDescription, globalTransaction, true)
						Expect(err).To(BeNil())

						globalTransactionManager.CommitTransaction(globalTransaction)
					})

					It("Returns an error if a migration is not actual and its siblings already have children", func() {
						globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
						Expect(err).To(BeNil())

						field := description.Field{
							Name:     "author",
							Type:     description.FieldTypeObject,
							LinkMeta: "staff",
							Optional: false,
						}

						migrationDescription := &migrations_description.MigrationDescription{
							Id:        "5",
							ApplyTo:   aMetaDescription.Name,
							DependsOn: []string{secondAppliedMigrationDescription.Id},
							Operations: [] migrations_description.MigrationOperationDescription{
								{
									Type:  migrations_description.AddFieldOperation,
									Field: &migrations_description.MigrationFieldDescription{Field: field},
								},
							},
						}

						err = migrationValidationService.Validate(migrationDescription, globalTransaction.DbTransaction)
						Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())
						Expect(err).NotTo(BeNil())
						Expect(err.(*migrations.MigrationError).Code()).To(Equal(migrations.MigrationIsNotActual))
					})
				})
			})
		})
	})
})
