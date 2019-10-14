package managers

import (
	. "github.com/onsi/ginkgo"
	"server/pg"
	"utils"
	pg_transactions "server/pg/transactions"
	. "github.com/onsi/gomega"
	"server/object/meta"
	"server/object/description"
	"server/transactions/file_transaction"
	"server/transactions"
	migrations_description "server/migrations/description"
)

var _ = Describe("MigrationManager`s rollback functionality", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
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
		//Flush meta/database
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		err = metaStore.Flush()
		Expect(err).To(BeNil())
		// drop history
		err = NewMigrationManager(
			metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
		).DropHistory(globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	}

	BeforeEach(flushDb)
	AfterEach(flushDb)

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

			aMetaDescription, err = migrationManager.Apply(firstAppliedMigrationDescription, globalTransaction, true)
			Expect(err).To(BeNil())

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("It can rollback `CreateObject` migration", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			_, err = migrationManager.rollback(firstAppliedMigrationDescription, globalTransaction, true)
			Expect(err).To(BeNil())

			//ensure migration description was removed
			_, err = migrationManager.migrationStorage.Get(firstAppliedMigrationDescription.Id)
			Expect(err).NotTo(BeNil())

			//ensure migration record was deleted
			migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			Expect(migrationRecord).To(BeNil())
			Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())

			// ensure operation was rolled back
			aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
			Expect(aMeta).To(BeNil())
			Expect(err).NotTo(BeNil())
		})

		Context("Having applied `addField` migration for object A", func() {
			var secondAppliedMigrationDescription *migrations_description.MigrationDescription

			BeforeEach(func() {
				//Create object A by applying a migration
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

				aMetaDescription, err = migrationManager.Apply(secondAppliedMigrationDescription, globalTransaction, true)
				Expect(err).To(BeNil())

				globalTransactionManager.CommitTransaction(globalTransaction)
			})

			It("It can rollback `AddField` migration", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				_, err = migrationManager.rollback(secondAppliedMigrationDescription, globalTransaction, true)
				Expect(err).To(BeNil())

				//ensure migration record was deleted
				migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())
				Expect(migrationRecord.Data["migration_id"]).To(Equal(firstAppliedMigrationDescription.Id))
				Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())

				// ensure operation was rolled back
				aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(aMeta.FindField("title")).To(BeNil())
			})

			It("It can rollback `RemoveField` migration", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

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

				aMetaDescription, err = migrationManager.Apply(appliedMigrationDescription, globalTransaction, true)
				Expect(err).To(BeNil())

				_, err = migrationManager.rollback(appliedMigrationDescription, globalTransaction, true)
				Expect(err).To(BeNil())

				//ensure migration record was deleted
				migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())
				Expect(migrationRecord.Data["migration_id"]).To(Equal(secondAppliedMigrationDescription.Id))
				Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())

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

					globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
					Expect(err).To(BeNil())
					aMetaDescription, err = migrationManager.Apply(thirdAppliedMigrationDescription, globalTransaction, true)
					Expect(err).To(BeNil())
					globalTransactionManager.CommitTransaction(globalTransaction)
				})

				It("It can rollback `UpdateField` migration", func() {
					globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
					Expect(err).To(BeNil())

					_, err = migrationManager.rollback(thirdAppliedMigrationDescription, globalTransaction, true)
					Expect(err).To(BeNil())

					//ensure migration record was deleted
					migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name, globalTransaction.DbTransaction)
					Expect(err).To(BeNil())
					Expect(migrationRecord.Data["migration_id"]).To(Equal(secondAppliedMigrationDescription.Id))
					Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())

					// ensure operation was rolled back
					aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
					Expect(err).To(BeNil())
					Expect(aMeta.FindField("new_title")).To(BeNil())
					Expect(aMeta.FindField("title")).NotTo(BeNil())
				})
			})
		})
		It("It can rollback `RenameObject` migration", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			updatedAMetaDescription := description.NewMetaDescription(
				"updated_a",
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

			secondAppliedMigrationDescription := &migrations_description.MigrationDescription{
				Id:        "2",
				ApplyTo:   "a",
				DependsOn: []string{firstAppliedMigrationDescription.Id},
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.RenameObjectOperation,
						MetaDescription: updatedAMetaDescription,
					},
				},
			}

			_, err = migrationManager.Apply(secondAppliedMigrationDescription, globalTransaction, true)
			Expect(err).To(BeNil())

			_, err = migrationManager.rollback(secondAppliedMigrationDescription, globalTransaction, true)
			Expect(err).To(BeNil())

			//ensure migration description was removed
			_, err = migrationManager.migrationStorage.Get(secondAppliedMigrationDescription.Id)
			Expect(err).NotTo(BeNil())

			//ensure migration record was deleted
			migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			Expect(migrationRecord.Data["migration_id"]).To(Equal(firstAppliedMigrationDescription.Id))
			Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())

			// ensure operation was rolled back
			aMeta, _, err := metaStore.Get("updated_a", false)
			Expect(aMeta).To(BeNil())
			Expect(err).NotTo(BeNil())

			aMeta, _, err = metaStore.Get(aMetaDescription.Name, false)
			Expect(aMeta).NotTo(BeNil())
			Expect(err).To(BeNil())
		})

		It("It can rollback `DeleteObject` migration", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			secondAppliedMigrationDescription := &migrations_description.MigrationDescription{
				Id:        "2",
				ApplyTo:   "a",
				DependsOn: []string{firstAppliedMigrationDescription.Id},
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.DeleteObjectOperation,
						MetaDescription: aMetaDescription,
					},
				},
			}

			_, err = migrationManager.Apply(secondAppliedMigrationDescription, globalTransaction, true)
			Expect(err).To(BeNil())

			_, err = migrationManager.rollback(secondAppliedMigrationDescription, globalTransaction, true)
			Expect(err).To(BeNil())

			//ensure migration description was removed
			_, err = migrationManager.migrationStorage.Get(secondAppliedMigrationDescription.Id)
			Expect(err).NotTo(BeNil())

			//ensure migration record was deleted
			migrationRecord, err := migrationManager.getLatestMigrationForObject(aMetaDescription.Name, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			Expect(migrationRecord.Data["migration_id"]).To(Equal(firstAppliedMigrationDescription.Id))
			Expect(globalTransactionManager.CommitTransaction(globalTransaction)).To(BeNil())

			// ensure operation was rolled back
			aMeta, _, err := metaStore.Get(aMetaDescription.Name, false)
			Expect(aMeta).NotTo(BeNil())
			Expect(err).To(BeNil())
		})
	})
})
