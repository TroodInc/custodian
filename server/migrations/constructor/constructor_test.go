package constructor

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"custodian/server/errors"
	"custodian/server/pg"
	"custodian/utils"
	"custodian/server/object/meta"
	"custodian/server/transactions/file_transaction"
	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions"
	"custodian/server/object/description"
	"custodian/server/pg/migrations/managers"
	migration_description "custodian/server/migrations/description"
	"custodian/server/pg_meta"
	"custodian/server/migrations"
)

var _ = Describe("Migration Constructor", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg_meta.NewPgMetaDescriptionSyncer(dbTransactionManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	migrationConstructor := NewMigrationConstructor(managers.NewMigrationManager(metaStore, dataManager, metaDescriptionSyncer,appConfig.MigrationStoragePath, globalTransactionManager))

	flushDb := func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	}

	AfterEach(flushDb)

	Describe("Separate operations` generation", func() {
		It("generates empty migration if nothing has changed", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			currentMetaDescription := &description.MetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
				Actions: []description.Action{},
				Cas:     false,
			}
			newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []migration_description.MigrationFieldDescription{
					{
						Field: description.Field{
							Name:     "id",
							Type:     description.FieldTypeString,
							Optional: false,
						},
						PreviousName: "",
					},
				},
				Actions: []migration_description.MigrationActionDescription{},
				Cas:     false,
			}
			_, err = migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
			Expect(err).NotTo(BeNil())
			Expect(err.(*errors.ServerError).Code).To(Equal(migrations.MigrationNoChangesWereDetected))

			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())
		})

		It("generates migration description with create operation if object is being created", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			newMetaDescription := &migration_description.MigrationMetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []migration_description.MigrationFieldDescription{
					{
						Field: description.Field{
							Name:     "id",
							Type:     description.FieldTypeString,
							Optional: false,
						},
						PreviousName: "",
					},
				},
				Actions: []migration_description.MigrationActionDescription{},
				Cas:     false,
			}
			migrationDescription, err := migrationConstructor.Construct(nil, newMetaDescription, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.CreateObjectOperation))

			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())
		})

		It("generates operation if object is being renamed", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			currentMetaDescription := &description.MetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
				Actions: []description.Action{},
				Cas:     false,
			}
			newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
				Name: "b",
				Key:  "id",
				Fields: []migration_description.MigrationFieldDescription{
					{
						Field: description.Field{
							Name:     "id",
							Type:     description.FieldTypeString,
							Optional: false,
						},
						PreviousName: "",
					},
				},
				Actions: []migration_description.MigrationActionDescription{},
				Cas:     false,
			}
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.RenameObjectOperation))

			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())

		})

		It("generates operation if object is being deleted", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			currentMetaDescription := &description.MetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
				Actions: []description.Action{},
				Cas:     false,
			}
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, nil, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.DeleteObjectOperation))

			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())

		})

		It("generates operation if field is being added", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			currentMetaDescription := &description.MetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
				Actions: []description.Action{},
				Cas:     false,
			}
			newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []migration_description.MigrationFieldDescription{
					{
						Field: description.Field{
							Name:     "id",
							Type:     description.FieldTypeString,
							Optional: false,
						},
						PreviousName: "",
					},
					{
						Field: description.Field{
							Name:     "new_field",
							Type:     description.FieldTypeString,
							Optional: false,
						},
						PreviousName: "",
					},
				},
				Actions: []migration_description.MigrationActionDescription{},
				Cas:     false,
			}
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.AddFieldOperation))

			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())
		})

		It("generates operation if field is being removed", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			currentMetaDescription := &description.MetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeString,
						Optional: false,
					},
					{
						Name:     "existing_field",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
				Actions: []description.Action{},
				Cas:     false,
			}
			newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []migration_description.MigrationFieldDescription{
					{
						Field: description.Field{
							Name:     "id",
							Type:     description.FieldTypeString,
							Optional: false,
						},
						PreviousName: "",
					},
				},
				Actions: []migration_description.MigrationActionDescription{},
				Cas:     false,
			}
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.RemoveFieldOperation))

			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())
		})

		Describe("Update field", func() {
			var currentMetaDescription *description.MetaDescription
			BeforeEach(func() {

				currentMetaDescription = &description.MetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []description.Field{
						{
							Name:     "id",
							Type:     description.FieldTypeString,
							Optional: false,
						},
						{
							Name:     "existing_field",
							Type:     description.FieldTypeString,
							Optional: false,
						},
						{
							Name:         "target_object",
							Type:         description.FieldTypeGeneric,
							LinkType:     description.LinkTypeInner,
							LinkMetaList: []string{"a"},
							Optional:     false,
						},
						{
							Name:     "created",
							Type:     description.FieldTypeDateTime,
							Optional: true,
						},
					},
					Actions: []description.Action{},
					Cas:     false,
				}
			})

			It("generates operation if field is being renamed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []migration_description.MigrationFieldDescription{
						{
							Field: description.Field{
								Name:     "id",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "",
						},
						{
							Field: description.Field{
								Name:     "new_field",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "existing_field",
						},
						{
							Field: description.Field{
								Name:         "target_object",
								Type:         description.FieldTypeGeneric,
								LinkType:     description.LinkTypeInner,
								LinkMetaList: []string{"a"},
								Optional:     false,
							},
						},
						{
							Field: description.Field{
								Name:     "created",
								Type:     description.FieldTypeDateTime,
								Optional: true,
							},
						},
					},
					Actions: []migration_description.MigrationActionDescription{},
					Cas:     false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s Optional value has changed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []migration_description.MigrationFieldDescription{
						{
							Field: description.Field{
								Name:     "id",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "",
						},

						{
							Field: description.Field{
								Name:     "existing_field",
								Type:     description.FieldTypeString,
								Optional: true,
							},
						},
						{
							Field: description.Field{
								Name:         "target_object",
								Type:         description.FieldTypeGeneric,
								LinkType:     description.LinkTypeInner,
								LinkMetaList: []string{"a"},
								Optional:     false,
							},
						},
						{
							Field: description.Field{
								Name:     "created",
								Type:     description.FieldTypeDateTime,
								Optional: true,
							},
						},
					},

					Actions: []migration_description.MigrationActionDescription{},
					Cas:     false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s Def value has changed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []migration_description.MigrationFieldDescription{
						{
							Field: description.Field{
								Name:     "id",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "",
						},
						{
							Field: description.Field{
								Name:     "existing_field",
								Type:     description.FieldTypeString,
								Optional: false,
								Def:      "some_value",
							},
						},
						{
							Field: description.Field{
								Name:         "target_object",
								Type:         description.FieldTypeGeneric,
								LinkType:     description.LinkTypeInner,
								LinkMetaList: []string{"a"},
								Optional:     false,
							},
						},
						{
							Field: description.Field{
								Name:     "created",
								Type:     description.FieldTypeDateTime,
								Optional: true,
							},
						},
					},
					Actions: []migration_description.MigrationActionDescription{},
					Cas:     false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s NowOnCreate value has changed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []migration_description.MigrationFieldDescription{
						{
							Field: description.Field{
								Name:     "id",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "",
						},
						{
							Field: description.Field{
								Name:     "existing_field",
								Type:     description.FieldTypeString,
								Optional: false,
							},
						},
						{
							Field: description.Field{
								Name:         "target_object",
								Type:         description.FieldTypeGeneric,
								LinkType:     description.LinkTypeInner,
								LinkMetaList: []string{"a"},
								Optional:     false,
							},
						},
						{
							Field: description.Field{
								Name:        "created",
								Type:        description.FieldTypeDateTime,
								NowOnCreate: true,
								Optional:    true,
							},
						},
					},
					Actions: []migration_description.MigrationActionDescription{},
					Cas:     false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s NowOnUpdate value has changed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []migration_description.MigrationFieldDescription{
						{
							Field: description.Field{
								Name:     "id",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "",
						},
						{
							Field: description.Field{
								Name:     "existing_field",
								Type:     description.FieldTypeString,
								Optional: false,
							},
						},
						{
							Field: description.Field{
								Name:         "target_object",
								Type:         description.FieldTypeGeneric,
								LinkType:     description.LinkTypeInner,
								LinkMetaList: []string{"a"},
								Optional:     false,
							},
						},
						{
							Field: description.Field{
								Name:        "created",
								Type:        description.FieldTypeDateTime,
								NowOnUpdate: true,
								Optional:    true,
							},
						},
					},
					Actions: []migration_description.MigrationActionDescription{},
					Cas:     false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s OnDelete value has changed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []migration_description.MigrationFieldDescription{
						{
							Field: description.Field{
								Name:     "id",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "",
						},
						{
							Field: description.Field{
								Name:     "existing_field",
								Type:     description.FieldTypeString,
								Optional: false,
								OnDelete: description.OnDeleteRestrictVerbose,
							},
						},
						{
							Field: description.Field{
								Name:         "target_object",
								Type:         description.FieldTypeGeneric,
								LinkType:     description.LinkTypeInner,
								LinkMetaList: []string{"a"},
								Optional:     false,
							},
						},
						{
							Field: description.Field{
								Name:     "created",
								Type:     description.FieldTypeDateTime,
								Optional: true,
							},
						},
					},
					Actions: []migration_description.MigrationActionDescription{},
					Cas:     false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s LinkMetaList value has changed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []migration_description.MigrationFieldDescription{
						{
							Field: description.Field{
								Name:     "id",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "",
						},
						{
							Field: description.Field{
								Name:     "existing_field",
								Type:     description.FieldTypeString,
								Optional: false,
							},
						},
						{
							Field: description.Field{
								Name:         "target_object",
								Type:         description.FieldTypeGeneric,
								LinkType:     description.LinkTypeInner,
								LinkMetaList: []string{"a", "b"},
								Optional:     false,
							},
						},
						{
							Field: description.Field{
								Name:     "created",
								Type:     description.FieldTypeDateTime,
								Optional: true,
							},
						},
					},
					Actions: []migration_description.MigrationActionDescription{},
					Cas:     false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})
		})

		It("generates operation if action is being added", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			currentMetaDescription := &description.MetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
				Actions: []description.Action{},
				Cas:     false,
			}
			newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []migration_description.MigrationFieldDescription{
					{
						Field: description.Field{
							Name:     "id",
							Type:     description.FieldTypeString,
							Optional: false,
						},
						PreviousName: "",
					},
				},
				Actions: []migration_description.MigrationActionDescription{{
					Action: description.Action{
						Name:     "new_action",
						Method:   description.MethodCreate,
						Protocol: description.REST,
						Args:     []string{"http://localhost:3000/some-handler"},
					},
				}},
				Cas: false,
			}
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.AddActionOperation))

			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())
		})

		It("generates operation if action is being removed", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			currentMetaDescription := &description.MetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
				Actions: []description.Action{
					{
						Name:     "new_action",
						Method:   description.MethodCreate,
						Protocol: description.REST,
						Args:     []string{"http://localhost:3000/some-handler"},
					},
				},
				Cas: false,
			}
			newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
				Name: "a",
				Key:  "id",
				Fields: []migration_description.MigrationFieldDescription{
					{
						Field: description.Field{
							Name:     "id",
							Type:     description.FieldTypeString,
							Optional: false,
						},
						PreviousName: "",
					},
				},
				Actions: []migration_description.MigrationActionDescription{},
				Cas:     false,
			}
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.RemoveActionOperation))

			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())
		})

		Describe("Update action", func() {
			var currentMetaDescription *description.MetaDescription
			BeforeEach(func() {

				currentMetaDescription = &description.MetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []description.Field{
						{
							Name:     "id",
							Type:     description.FieldTypeString,
							Optional: false,
						},
					},
					Actions: []description.Action{
						{
							Name:     "new_action",
							Method:   description.MethodCreate,
							Protocol: description.REST,
							Args:     []string{"http://localhost:3000/some-handler"},
						},
					},
					Cas: false,
				}
			})

			It("generates operation if action is being renamed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []migration_description.MigrationFieldDescription{
						{
							Field: description.Field{
								Name:     "id",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "",
						},
					},
					Actions: []migration_description.MigrationActionDescription{
						{
							Action: description.Action{
								Name:     "updated_action",
								Method:   description.MethodCreate,
								Protocol: description.REST,
								Args:     []string{"http://localhost:3000/some-handler"},
							},
							PreviousName: "new_action",
						},
					},
					Cas: false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})

			It("generates operation if action`s Method value has changed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []migration_description.MigrationFieldDescription{
						{
							Field: description.Field{
								Name:     "id",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "",
						},
					},
					Actions: []migration_description.MigrationActionDescription{
						{
							Action: description.Action{
								Name:     "new_action",
								Method:   description.MethodCreate,
								Protocol: description.TEST,
								Args:     []string{"http://localhost:3000/some-handler"},
							},
						},
					},
					Cas: false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})

			It("generates operation if action`s Args value has changed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []migration_description.MigrationFieldDescription{
						{
							Field: description.Field{
								Name:     "id",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "",
						},
					},
					Actions: []migration_description.MigrationActionDescription{
						{
							Action: description.Action{
								Name:     "new_action",
								Method:   description.MethodCreate,
								Protocol: description.REST,
								Args:     []string{"http://localhost:3000/some-another-handler"},
							},
						},
					},
					Cas: false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})

			It("generates operation if action`s ActiveIfNotRoot value has changed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: "a",
					Key:  "id",
					Fields: []migration_description.MigrationFieldDescription{
						{
							Field: description.Field{
								Name:     "id",
								Type:     description.FieldTypeString,
								Optional: false,
							},
							PreviousName: "",
						},
					},
					Actions: []migration_description.MigrationActionDescription{
						{
							Action: description.Action{
								Name:            "new_action",
								Method:          description.MethodCreate,
								Protocol:        description.REST,
								Args:            []string{"http://localhost:3000/some-handler"},
								ActiveIfNotRoot: true,
							},
						},
					},
					Cas: false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction.DbTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})
		})
	})
})
