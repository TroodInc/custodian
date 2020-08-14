package managers

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	migrations_description "custodian/server/migrations/description"
	"custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/pg"
	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions"
	"custodian/server/transactions/file_transaction"
	"custodian/utils"
)

var _ = Describe("MigrationManager`s rollback functionality", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	migrationManager := NewMigrationManager(
		metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
	)

	flushDb := func() {
		// drop history
		err := migrationManager.DropHistory()
		Expect(err).To(BeNil())
		//Flush meta/database
		err = metaStore.Flush()
		Expect(err).To(BeNil())
	}

	BeforeEach(flushDb)
	AfterEach(flushDb)

	Context("Having applied `create` migration for object A", func() {
		var aMetaDescription *description.MetaDescription
		var firstAppliedMigrationDescription *migrations_description.MigrationDescription

		BeforeEach(func() {
			//Create object A by applying a migration
			aMetaDescription = object.GetBaseMetaData(utils.RandomString(8))

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

		It("It can rollback `CreateObject` migration", func() {
			_, err := migrationManager.rollback(firstAppliedMigrationDescription, true)
			Expect(err).To(BeNil())

			//ensure migration description was removed
			migration, err := migrationManager.Get(firstAppliedMigrationDescription.Id)
			Expect(migration).To(BeNil())

			//ensure migration record was deleted
			migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name)
			Expect(err).To(BeNil())
			Expect(migrationRecord).To(BeNil())

			// ensure operation was rolled back
			aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
			Expect(aMeta).To(BeNil())
			Expect(err).NotTo(BeNil())
		})

		Context("Having applied `addField` migration for object A", func() {
			var secondAppliedMigrationDescription *migrations_description.MigrationDescription

			BeforeEach(func() {
				//Create object A by applying a migration
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

				var err error
				aMetaDescription, err = migrationManager.Apply(secondAppliedMigrationDescription,true, false)
				Expect(err).To(BeNil())
			})

			It("It can rollback `AddField` migration", func() {
				_, err := migrationManager.rollback(secondAppliedMigrationDescription, true)
				Expect(err).To(BeNil())

				//ensure migration record was deleted
				migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name)
				Expect(err).To(BeNil())
				Expect(migrationRecord.Data["id"]).To(Equal(firstAppliedMigrationDescription.Id))

				// ensure operation was rolled back
				aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(aMeta.FindField("title")).To(BeNil())
			})

			It("It can rollback `RemoveField` migration", func() {
				field := description.Field{
					Name:     "title",
					Type:     description.FieldTypeString,
					Optional: false,
				}

				appliedMigrationDescription := &migrations_description.MigrationDescription{
					Id:        "3",
					ApplyTo:   aMetaDescription.Name,
					DependsOn: []string{secondAppliedMigrationDescription.Id},
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.RemoveFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				var err error
				aMetaDescription, err = migrationManager.Apply(appliedMigrationDescription, true, false)
				Expect(err).To(BeNil())

				_, err = migrationManager.rollback(appliedMigrationDescription, true)
				Expect(err).To(BeNil())

				//ensure migration record was deleted
				migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name)
				Expect(err).To(BeNil())
				Expect(migrationRecord.Data["id"]).To(Equal(secondAppliedMigrationDescription.Id))

				// ensure operation was rolled back
				aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(aMeta.FindField("title")).NotTo(BeNil())
			})

			Context("Having applied `UpdateField` migration for object A", func() {
				var thirdAppliedMigrationDescription *migrations_description.MigrationDescription

				BeforeEach(func() {
					//Create object A by applying a migration
					field := description.Field{
						Name:     "new_title",
						Type:     description.FieldTypeString,
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

				It("It can rollback `UpdateField` migration", func() {
					_, err := migrationManager.rollback(thirdAppliedMigrationDescription, true)
					Expect(err).To(BeNil())

					//ensure migration record was deleted
					migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name)
					Expect(err).To(BeNil())
					Expect(migrationRecord.Data["id"]).To(Equal(secondAppliedMigrationDescription.Id))

					// ensure operation was rolled back
					aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
					Expect(err).To(BeNil())
					Expect(aMeta.FindField("new_title")).To(BeNil())
					Expect(aMeta.FindField("title")).NotTo(BeNil())
				})
			})
		})
		It("It can rollback `RenameObject` migration", func() {
			updatedAMetaDescription := object.GetBaseMetaData(utils.RandomString(8))

			secondAppliedMigrationDescription := &migrations_description.MigrationDescription{
				Id:        "2",
				ApplyTo:   aMetaDescription.Name,
				DependsOn: []string{firstAppliedMigrationDescription.Id},
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.RenameObjectOperation,
						MetaDescription: updatedAMetaDescription,
					},
				},
			}

			_, err := migrationManager.Apply(secondAppliedMigrationDescription, true, false)
			Expect(err).To(BeNil())

			_, err = migrationManager.rollback(secondAppliedMigrationDescription, true)
			Expect(err).To(BeNil())

			//ensure migration description was removed
			migration, err := migrationManager.Get(secondAppliedMigrationDescription.Id)
			Expect(migration).To(BeNil())

			//ensure migration record was deleted
			migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name)
			Expect(err).To(BeNil())
			Expect(migrationRecord.Data["id"]).To(Equal(firstAppliedMigrationDescription.Id))

			// ensure operation was rolled back
			aMeta, _, err := metaStore.Get("updated_a", false)
			Expect(aMeta).To(BeNil())
			Expect(err).NotTo(BeNil())

			aMeta, _, err = metaStore.Get(aMetaDescription.Name, false)
			Expect(aMeta).NotTo(BeNil())
			Expect(err).To(BeNil())
		})

		It("It can rollback `DeleteObject` migration", func() {
			secondAppliedMigrationDescription := &migrations_description.MigrationDescription{
				Id:        "2",
				ApplyTo:   aMetaDescription.Name,
				DependsOn: []string{firstAppliedMigrationDescription.Id},
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.DeleteObjectOperation,
						MetaDescription: aMetaDescription,
					},
				},
			}

			_, err := migrationManager.Apply(secondAppliedMigrationDescription, true, false)
			Expect(err).To(BeNil())

			_, err = migrationManager.rollback(secondAppliedMigrationDescription, true)
			Expect(err).To(BeNil())

			//ensure migration description was removed
			migration, err := migrationManager.Get(secondAppliedMigrationDescription.Id)
			Expect(migration).To(BeNil())

			//ensure migration record was deleted
			migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name)
			Expect(err).To(BeNil())
			Expect(migrationRecord.Data["id"]).To(Equal(firstAppliedMigrationDescription.Id))

			// ensure operation was rolled back
			aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
			Expect(aMeta).NotTo(BeNil())
			Expect(err).To(BeNil())
		})
	})
})
