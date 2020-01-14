package constructor

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/errors"
	"server/pg"
	"utils"
	"server/object/meta"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/object/description"
	"server/pg/migrations/managers"
	migration_description "server/migrations/description"

	"server/migrations"
)

var _ = Describe("Migration Constructor", func() {
	appConfig := utils.GetConfig()
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

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
	migrationConstructor := NewMigrationConstructor(migrationManager)

	BeforeEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	Describe("Separate operations` generation", func() {
		It("generates empty migration if nothing has changed", func() {
			currentMetaDescription := description.GetBasicMetaDescription("random")
			newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
				Name: currentMetaDescription.Name,
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
			_, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
			Expect(err).NotTo(BeNil())
			Expect(err.(*errors.ServerError).Code).To(Equal(migrations.MigrationNoChangesWereDetected))
		})

		It("generates migration description with create operation if object is being created", func() {
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
			migrationDescription, err := migrationConstructor.Construct(nil, newMetaDescription)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.CreateObjectOperation))
		})

		It("generates operation if object is being renamed", func() {
			currentMetaDescription := description.GetBasicMetaDescription("random")
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
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.RenameObjectOperation))
		})

		It("generates operation if object is being deleted", func() {
			currentMetaDescription := description.GetBasicMetaDescription("random")
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, nil)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.DeleteObjectOperation))
		})

		It("generates operation if field is being added", func() {
			currentMetaDescription := description.GetBasicMetaDescription("random")
			newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
				Name: currentMetaDescription.Name,
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
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.AddFieldOperation))
		})

		It("generates operation if field is being removed", func() {
			currentMetaDescription := description.GetBasicMetaDescription("random")
			currentMetaDescription.Fields = append(currentMetaDescription.Fields, description.Field{
				Name:     "existing_field",
				Type:     description.FieldTypeString,
				Optional: false,
			})

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
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.RemoveFieldOperation))
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))
			})

			It("generates operation if field`s Optional value has changed", func() {
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))
			})

			It("generates operation if field`s Def value has changed", func() {
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))
			})

			It("generates operation if field`s NowOnCreate value has changed", func() {
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))
			})

			It("generates operation if field`s NowOnUpdate value has changed", func() {
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))
			})

			It("generates operation if field`s OnDelete value has changed", func() {
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))
			})

			It("generates operation if field`s LinkMetaList value has changed", func() {
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))
			})
		})

		It("generates operation if action is being added", func() {
			currentMetaDescription := description.GetBasicMetaDescription("random")
			newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
				Name: currentMetaDescription.Name,
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
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.AddActionOperation))
		})

		It("generates operation if action is being removed", func() {
			currentMetaDescription := description.GetBasicMetaDescription("random")
			currentMetaDescription.Actions = append(currentMetaDescription.Actions, description.Action{
				Name:     "new_action",
				Method:   description.MethodCreate,
				Protocol: description.REST,
				Args:     []string{"http://localhost:3000/some-handler"},
			})

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
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.RemoveActionOperation))
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))
			})

			It("generates operation if action`s Method value has changed", func() {
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))
			})

			It("generates operation if action`s Args value has changed", func() {
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))
			})

			It("generates operation if action`s ActiveIfNotRoot value has changed", func() {
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))
			})
		})
	})
})
