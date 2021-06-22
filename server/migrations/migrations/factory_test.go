package migrations_test

import (
	migrations_description "custodian/server/migrations/description"
	. "custodian/server/migrations/migrations"
	object2 "custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/migrations/managers"
	"custodian/server/object/migrations/operations/field"
	"custodian/server/object/migrations/operations/object"

	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Migration Factory", func() {
	appConfig := utils.GetConfig()
	db, _ := object2.NewDbConnection(appConfig.DbConnectionUrl)

	dbTransactionManager := object2.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object2.NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := object2.NewStore(metaDescriptionSyncer, dbTransactionManager)

	migrationManager := managers.NewMigrationManager(
		metaStore, dbTransactionManager,
	)

	var metaDescription *description.MetaDescription

	flushDb := func() {
		//Flush meta/database
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	}

	AfterEach(flushDb)

	testObjAName := utils.RandomString(8)
	testObjBName := utils.RandomString(8)

	//setup MetaDescription
	JustBeforeEach(func() {
		metaDescription = &description.MetaDescription{
			Name: testObjAName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "date",
					Type:     description.FieldTypeDate,
					Optional: false,
				},
			},
		}
		//create MetaDescription
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())
		//sync MetaDescription

		operation := object.NewCreateObjectOperation(metaDescription)

		metaDescription, err = operation.SyncMetaDescription(nil, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		//sync DB
		err = operation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		//
		err = dbTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
	})

	It("factories migration containing CreateObjectOperation", func() {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   "",
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.CreateObjectOperation,
					MetaDescription: metaDescription,
				},
			},
		}

		migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*object.CreateObjectOperation)
		Expect(ok).To(BeTrue())

		dbTransactionManager.CommitTransaction(globalTransaction)
	})

	It("factories migration containing RenameObjectOperation", func() {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		updatedMetaDescription := metaDescription.Clone()
		updatedMetaDescription.Name = "updatedA"

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   testObjAName,
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.RenameObjectOperation,
					MetaDescription: updatedMetaDescription,
				},
			},
		}

		migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*object.RenameObjectOperation)
		Expect(ok).To(BeTrue())

		dbTransactionManager.CommitTransaction(globalTransaction)
	})

	It("factories migration containing RenameObjectOperation", func() {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   testObjAName,
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.DeleteObjectOperation,
					MetaDescription: metaDescription,
				},
			},
		}

		migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*object.DeleteObjectOperation)
		Expect(ok).To(BeTrue())

		dbTransactionManager.CommitTransaction(globalTransaction)
	})

	It("factories migration containing AddFieldOperation", func() {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   testObjAName,
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:  migrations_description.AddFieldOperation,
					Field: &migrations_description.MigrationFieldDescription{Field: description.Field{Name: "new-field", Type: description.FieldTypeString, Optional: true}},
				},
			},
		}

		migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*field.AddFieldOperation)
		Expect(ok).To(BeTrue())

		dbTransactionManager.CommitTransaction(globalTransaction)
	})

	It("factories migration containing RemoveFieldOperation", func() {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   testObjAName,
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:  migrations_description.RemoveFieldOperation,
					Field: &migrations_description.MigrationFieldDescription{Field: description.Field{Name: "date"}},
				},
			},
		}

		migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*field.RemoveFieldOperation)
		Expect(ok).To(BeTrue())

		dbTransactionManager.CommitTransaction(globalTransaction)
	})

	It("factories migration containing UpdateFieldOperation", func() {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   testObjAName,
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:  migrations_description.RemoveFieldOperation,
					Field: &migrations_description.MigrationFieldDescription{Field: description.Field{Name: "date"}},
				},
			},
		}

		migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*field.RemoveFieldOperation)
		Expect(ok).To(BeTrue())

		dbTransactionManager.CommitTransaction(globalTransaction)
	})

	Describe("Automated generic fields` migrations` spawning", func() {
		It("adds reverse generic outer link while object is being created", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			bMetaDescription := description.NewMetaDescription(
				testObjBName,
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
						Name:         "target_object",
						Type:         description.FieldTypeGeneric,
						LinkType:     description.LinkTypeInner,
						LinkMetaList: []string{testObjAName},
						Optional:     false,
					},
				},
				nil,
				false,
			)

			migrationDescription := &migrations_description.MigrationDescription{
				Id:        "some-unique-id",
				ApplyTo:   "",
				DependsOn: nil,
				Operations: []migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: bMetaDescription,
					},
				},
			}

			migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)

			Expect(err).To(BeNil())
			Expect(migration.RunAfter).To(HaveLen(1))
			Expect(migration.RunAfter[0].Operations).To(HaveLen(1))
			Expect(migration.RunAfter[0].Operations[0].Field.Name).To(Equal(object2.ReverseInnerLinkName(testObjBName)))

			dbTransactionManager.CommitTransaction(globalTransaction)
		})

		Context("having object B", func() {
			var bMetaDescription *description.MetaDescription
			BeforeEach(func() {
				bMetaDescription = description.NewMetaDescription(
					testObjBName,
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
				bMetaObj, err := object2.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(bMetaDescription)
				Expect(err).To(BeNil())

				err = metaStore.Create(bMetaObj)
				Expect(err).To(BeNil())
			})

			It("adds a reverse generic outer link when a new inner generic field is being added to an object", func() {
				globalTransaction, err := dbTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjAName},
					Optional:     false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: []migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)

				Expect(err).To(BeNil())
				Expect(migration.RunAfter).To(HaveLen(1))
				Expect(migration.RunAfter[0].Operations).To(HaveLen(1))
				Expect(migration.RunAfter[0].Operations[0].Field.Name).To(Equal(object2.ReverseInnerLinkName(testObjBName)))

				err = dbTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())

			})

			It("removes and adds reverse generic outer links while inner generic field`s LinkMetaList is being updated", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjAName},
					Optional:     false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: []migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				_, err := migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				cMetaDescription := description.NewMetaDescription(
					"c",
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
				cMetaObj, err := object2.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(cMetaDescription)
				Expect(err).To(BeNil())

				err = metaStore.Create(cMetaObj)
				Expect(err).To(BeNil())

				//LinkMetaList is being changed
				field = description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{"c"},
					Optional:     false,
				}

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: []migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.UpdateFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field, PreviousName: field.Name},
						},
					},
				}

				migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)
				Expect(err).To(BeNil())

				Expect(migration.RunBefore).To(HaveLen(1))
				Expect(migration.RunBefore[0].Operations).To(HaveLen(1))
				Expect(migration.RunBefore[0].Operations[0].Type).To(Equal(migrations_description.RemoveFieldOperation))
				Expect(migration.RunBefore[0].Operations[0].Field.Name).To(Equal(object2.ReverseInnerLinkName(testObjBName)))

				Expect(migration.RunAfter).To(HaveLen(1))
				Expect(migration.RunAfter[0].Operations).To(HaveLen(1))
				Expect(migration.RunAfter[0].Operations[0].Type).To(Equal(migrations_description.AddFieldOperation))
				Expect(migration.RunAfter[0].Operations[0].Field.Name).To(Equal(object2.ReverseInnerLinkName(testObjBName)))
			})

			It("renames reverse generic outer links if object which owns inner generic link is being renamed", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjAName},
					Optional:     false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: []migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				_, err := migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				renamedBMetaDescription := bMetaDescription.Clone()
				renamedBMetaDescription.Name = "bb"

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: []migrations_description.MigrationOperationDescription{
						{
							Type:            migrations_description.RenameObjectOperation,
							MetaDescription: renamedBMetaDescription,
						},
					},
				}

				migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)
				Expect(err).To(BeNil())

				Expect(migration.RunAfter).To(HaveLen(1))
				Expect(migration.RunAfter[0].Operations).To(HaveLen(1))
				Expect(migration.RunAfter[0].Operations[0].Type).To(Equal(migrations_description.UpdateFieldOperation))
				Expect(migration.RunAfter[0].Operations[0].Field.Name).To(Equal(object2.ReverseInnerLinkName("bb")))
			})

			It("removes generic outer links if object which owns inner generic link is being deleted", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjAName},
					Optional:     false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: []migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				_, err := migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: []migrations_description.MigrationOperationDescription{
						{
							Type:            migrations_description.DeleteObjectOperation,
							MetaDescription: bMetaDescription,
						},
					},
				}

				migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)
				Expect(err).To(BeNil())

				Expect(migration.RunBefore).To(HaveLen(1))
				Expect(migration.RunBefore[0].Operations).To(HaveLen(1))
				Expect(migration.RunBefore[0].Operations[0].Type).To(Equal(migrations_description.RemoveFieldOperation))
				Expect(migration.RunBefore[0].Operations[0].Field.Name).To(Equal(object2.ReverseInnerLinkName(testObjBName)))
			})

			It("removes generic outer links if inner generic link is being removed", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjAName},
					Optional:     false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: []migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				_, err := migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: []migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.RemoveFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)
				Expect(err).To(BeNil())

				Expect(migration.RunBefore).To(HaveLen(1))
				Expect(migration.RunBefore[0].Operations).To(HaveLen(1))
				Expect(migration.RunBefore[0].Operations[0].Type).To(Equal(migrations_description.RemoveFieldOperation))
				Expect(migration.RunBefore[0].Operations[0].Field.Name).To(Equal(object2.ReverseInnerLinkName(testObjBName)))
			})
		})
	})
})
