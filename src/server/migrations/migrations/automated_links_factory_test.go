package migrations_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	migrations_description "server/migrations/description"
	. "server/migrations/migrations"
	"server/object"
	"server/object/meta"

	"server/pg"
	"server/pg/migrations/managers"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("Automated generic links` migrations` spawning", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewMetaStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	migrationManager := managers.NewMigrationManager(metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager)

	var metaDescription *meta.Meta

	flushDb := func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	}

	BeforeEach(flushDb)
	AfterEach(flushDb)

	//setup MetaDescription
	JustBeforeEach(func() {
		metaDescription = object.GetBaseMetaData(utils.RandomString(8))
		//create MetaDescription
		metaObj, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(metaDescription)
		Expect(err).To(BeNil())

		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
	})

	Describe("Automated links` migrations` spawning", func() {
		It("adds reverse outer link while object is being created", func() {
			bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields, &meta.Field{
				Name:     "a",
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				LinkMeta: metaDescription,
				Optional: false,
			})

			migrationDescription := &migrations_description.MigrationDescription{
				Id:        "some-unique-id",
				ApplyTo:   "",
				DependsOn: nil,
				Operations: [] migrations_description.MigrationOperationDescription{
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
			Expect(migration.RunAfter[0].Operations[0].Field.Name).To(Equal(meta.ReverseInnerLinkName("b")))
		})

		It("replaces automatically added reverse outer link with explicitly specified new one", func() {
			bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields, &meta.Field{
				Name:     "a",
				Type:     meta.FieldTypeObject,
				LinkType: meta.LinkTypeInner,
				LinkMeta: metaDescription,
				Optional: false,
			})

			migrationDescription := &migrations_description.MigrationDescription{
				Id:        "some-unique-id",
				ApplyTo:   "",
				DependsOn: nil,
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: bMetaDescription,
					},
				},
			}

			_, err := migrationManager.Apply(migrationDescription, false, false)
			Expect(err).To(BeNil())

			aMetaMap, _, err := metaDescriptionSyncer.Get("a")
			aMetaDescription := meta.NewMetaFromMap(aMetaMap)
			Expect(err).To(BeNil())
			Expect(aMetaDescription.FindField("b_set")).NotTo(BeNil())

			migrationDescription = &migrations_description.MigrationDescription{
				Id:        "some-unique-id",
				ApplyTo:   "a",
				DependsOn: nil,
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type: migrations_description.AddFieldOperation,
						Field: &migrations_description.MigrationFieldDescription{
							Field: meta.Field{
								Name:           "explicitly_set_b_set",
								Type:           meta.FieldTypeArray,
								LinkType:       meta.LinkTypeOuter,
								OuterLinkField: bMetaDescription.FindField("a"),
								LinkMeta:       bMetaDescription,
							},
						},
					},
				},
			}

			migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)
			Expect(err).To(BeNil())

			Expect(migration.RunBefore).To(HaveLen(1))
			Expect(migration.RunBefore[0].Operations[0].Type).To(Equal(migrations_description.RemoveFieldOperation))
			Expect(migration.RunBefore[0].Operations[0].Field.Name).To(Equal(meta.ReverseInnerLinkName("b")))
		})

		Context("having object B", func() {
			var bMetaDescription *meta.Meta
			BeforeEach(func() {
				bMetaDescription = object.GetBaseMetaData(utils.RandomString(8))
				bMetaObj, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(bMetaDescription)
				Expect(err).To(BeNil())

				err = metaStore.Create(bMetaObj)
				Expect(err).To(BeNil())
			})

			It("adds a reverse outer link when a new inner link field is being added to an object", func() {
				field := meta.Field{
					Name:     "a",
					Type:     meta.FieldTypeObject,
					LinkType: meta.LinkTypeInner,
					LinkMeta: metaDescription,
					Optional: false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
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
				Expect(migration.RunAfter[0].Operations[0].Field.Name).To(Equal(meta.ReverseInnerLinkName("b")))
			})

			It("renames reverse outer link if object which owns inner link is being renamed", func() {
				field := meta.Field{
					Name:     "a",
					Type:     meta.FieldTypeObject,
					LinkType: meta.LinkTypeInner,
					LinkMeta: metaDescription,
					Optional: false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
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
					Operations: [] migrations_description.MigrationOperationDescription{
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
				Expect(migration.RunAfter[0].Operations[0].Field.Name).To(Equal(meta.ReverseInnerLinkName("bb")))
			})

			It("removes outer links if object which owns inner link is being deleted", func() {
				field := meta.Field{
					Name:     "a",
					Type:     meta.FieldTypeObject,
					LinkType: meta.LinkTypeInner,
					LinkMeta: metaDescription,
					Optional: false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				_, err := migrationManager.Apply(migrationDescription,false, false)
				Expect(err).To(BeNil())

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
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
				Expect(migration.RunBefore[0].Operations[0].Field.Name).To(Equal(meta.ReverseInnerLinkName("b")))
			})

			It("removes outer links if inner link is being removed", func() {
				field := meta.Field{
					Name:     "a",
					Type:     meta.FieldTypeObject,
					LinkType: meta.LinkTypeInner,
					LinkMeta: metaDescription,
					Optional: false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				_, err := migrationManager.Apply(migrationDescription,false, false)
				Expect(err).To(BeNil())

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "some-unique-id",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
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
				Expect(migration.RunBefore[0].Operations[0].Field.Name).To(Equal(meta.ReverseInnerLinkName("b")))
			})
		})
	})
})
