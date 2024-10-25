package constructor

import (
	"custodian/server/errors"
	"custodian/server/migrations"
	migration_description "custodian/server/migrations/description"
	"custodian/server/noti"
	"custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/migrations/managers"
	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Migration Constructor", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache(), db)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)

	migrationConstructor := NewMigrationConstructor(managers.NewMigrationManager(metaDescriptionSyncer, dbTransactionManager, db))

	flushDb := func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	}
	testObjAName := utils.RandomString(8)

	AfterEach(flushDb)

	Describe("Separate operations` generation", func() {
		It("generates empty migration if nothing has changed", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())
			testObjAName := utils.RandomString(8)

			currentMetaDescription := &description.MetaDescription{
				Name: testObjAName,
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
				Name: testObjAName,
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
			_, err = migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
			Expect(err).NotTo(BeNil())
			Expect(err.(*errors.ServerError).Code).To(Equal(migrations.MigrationNoChangesWereDetected))

			err = globalTransaction.Commit()
			Expect(err).To(BeNil())
		})

		It("generates migration description with create operation if object is being created", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())
			testObjAName := utils.RandomString(8)

			newMetaDescription := &migration_description.MigrationMetaDescription{
				Name: testObjAName,
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
			migrationDescription, err := migrationConstructor.Construct(nil, newMetaDescription, globalTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.CreateObjectOperation))

			err = globalTransaction.Commit()
			Expect(err).To(BeNil())
		})

		It("generates operation if object is being renamed", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())
			testObjAName := utils.RandomString(8)
			testObjBName := utils.RandomString(8)

			currentMetaDescription := &description.MetaDescription{
				Name: testObjAName,
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
				Name: testObjBName,
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
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.RenameObjectOperation))

			err = globalTransaction.Commit()
			Expect(err).To(BeNil())

		})

		It("generates operation if object is being deleted", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())
			testObjAName := utils.RandomString(8)

			currentMetaDescription := &description.MetaDescription{
				Name: testObjAName,
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
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, nil, globalTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.DeleteObjectOperation))

			err = globalTransaction.Commit()
			Expect(err).To(BeNil())

		})

		It("generates operation if field is being added", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())
			testObjAName := utils.RandomString(8)

			currentMetaDescription := &description.MetaDescription{
				Name: testObjAName,
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
				Name: testObjAName,
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
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.AddFieldOperation))

			err = globalTransaction.Commit()
			Expect(err).To(BeNil())
		})

		It("generates operation if field is being removed", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())
			testObjAName := utils.RandomString(8)

			currentMetaDescription := &description.MetaDescription{
				Name: testObjAName,
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
				Name: testObjAName,
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
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.RemoveFieldOperation))

			err = globalTransaction.Commit()
			Expect(err).To(BeNil())
		})

		Describe("Update field", func() {
			var currentMetaDescription *description.MetaDescription

			BeforeEach(func() {

				currentMetaDescription = &description.MetaDescription{
					Name: testObjAName,
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
							LinkMetaList: []string{testObjAName},
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
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: testObjAName,
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
								LinkMetaList: []string{testObjAName},
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransaction.Commit()
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s Optional value has changed", func() {
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: testObjAName,
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
								LinkMetaList: []string{testObjAName},
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransaction.Commit()
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s Def value has changed", func() {
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: testObjAName,
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
								LinkMetaList: []string{testObjAName},
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransaction.Commit()
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s NowOnCreate value has changed", func() {
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: testObjAName,
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
								LinkMetaList: []string{testObjAName},
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransaction.Commit()
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s NowOnUpdate value has changed", func() {
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: testObjAName,
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
								LinkMetaList: []string{testObjAName},
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransaction.Commit()
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s OnDelete value has changed", func() {
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: testObjAName,
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
								LinkMetaList: []string{testObjAName},
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransaction.Commit()
				Expect(err).To(BeNil())
			})

			It("generates operation if field`s LinkMetaList value has changed", func() {
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: testObjAName,
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
								LinkMetaList: []string{testObjAName, "b"},
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
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateFieldOperation))

				err = globalTransaction.Commit()
				Expect(err).To(BeNil())
			})
		})

		It("generates operation if action is being added", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			currentMetaDescription := &description.MetaDescription{
				Name: testObjAName,
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
				Name: testObjAName,
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
						Protocol: noti.REST,
						Args:     []string{"http://localhost:3000/some-handler"},
					},
				}},
				Cas: false,
			}
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.AddActionOperation))

			err = globalTransaction.Commit()
			Expect(err).To(BeNil())
		})

		It("generates operation if action is being removed", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			currentMetaDescription := &description.MetaDescription{
				Name: testObjAName,
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
						Protocol: noti.REST,
						Args:     []string{"http://localhost:3000/some-handler"},
					},
				},
				Cas: false,
			}
			newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
				Name: testObjAName,
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
			migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
			Expect(err).To(BeNil())

			Expect(migrationDescription).NotTo(BeNil())
			Expect(migrationDescription.Operations).To(HaveLen(1))
			Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.RemoveActionOperation))

			err = globalTransaction.Commit()
			Expect(err).To(BeNil())
		})

		Describe("Update action", func() {
			var currentMetaDescription *description.MetaDescription
			BeforeEach(func() {

				currentMetaDescription = &description.MetaDescription{
					Name: testObjAName,
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
							Protocol: noti.REST,
							Args:     []string{"http://localhost:3000/some-handler"},
						},
					},
					Cas: false,
				}
			})

			It("generates operation if action is being renamed", func() {
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: testObjAName,
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
								Protocol: noti.REST,
								Args:     []string{"http://localhost:3000/some-handler"},
							},
							PreviousName: "new_action",
						},
					},
					Cas: false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))

				err = globalTransaction.Commit()
				Expect(err).To(BeNil())
			})

			It("generates operation if action`s Method value has changed", func() {
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: testObjAName,
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
								Protocol: noti.TEST,
								Args:     []string{"http://localhost:3000/some-handler"},
							},
						},
					},
					Cas: false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))

				err = globalTransaction.Commit()
				Expect(err).To(BeNil())
			})

			It("generates operation if action`s Args value has changed", func() {
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: testObjAName,
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
								Protocol: noti.REST,
								Args:     []string{"http://localhost:3000/some-another-handler"},
							},
						},
					},
					Cas: false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))

				err = globalTransaction.Commit()
				Expect(err).To(BeNil())
			})

			It("generates operation if action`s ActiveIfNotRoot value has changed", func() {
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				newMetaMigrationDescription := &migration_description.MigrationMetaDescription{
					Name: testObjAName,
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
								Protocol:        noti.REST,
								Args:            []string{"http://localhost:3000/some-handler"},
								ActiveIfNotRoot: true,
							},
						},
					},
					Cas: false,
				}
				migrationDescription, err := migrationConstructor.Construct(currentMetaDescription, newMetaMigrationDescription, globalTransaction)
				Expect(err).To(BeNil())

				Expect(migrationDescription).NotTo(BeNil())
				Expect(migrationDescription.Operations).To(HaveLen(1))
				Expect(migrationDescription.Operations[0].Type).To(Equal(migration_description.UpdateActionOperation))

				err = globalTransaction.Commit()
				Expect(err).To(BeNil())
			})
		})
	})
})
