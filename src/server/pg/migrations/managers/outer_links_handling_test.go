package managers

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/meta"

	migrations_description "server/migrations/description"
	"server/object"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("Outer links spawned migrations appliance", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewMetaStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	migrationManager := NewMigrationManager(
		metaStore, dataManager, metaDescriptionSyncer, appConfig.MigrationStoragePath, globalTransactionManager,
	)

	var metaDescription *meta.Meta

	flushDb := func() {
		//Flush meta/database
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

	Describe("Spawned migrations` appliance", func() {
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

			_, err := migrationManager.Apply(migrationDescription,false, false)
			Expect(err).To(BeNil())

			aMetaObj, _, err := metaStore.Get(metaDescription.Name, false)
			Expect(aMetaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name))).NotTo(BeNil())
			Expect(aMetaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name)).LinkMeta.Name).To(Equal(bMetaDescription.Name))
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

			_, err := migrationManager.Apply(migrationDescription,false, false)
			Expect(err).To(BeNil())

			aMetaMap, _, err := metaDescriptionSyncer.Get(metaDescription.Name)
			aMetaDescription := meta.NewMetaFromMap(aMetaMap)
			Expect(err).To(BeNil())
			Expect(aMetaDescription.FindField(bMetaDescription.Name + "_set")).NotTo(BeNil())

			migrationDescription = &migrations_description.MigrationDescription{
				Id:        "some-unique-id",
				ApplyTo:   aMetaDescription.Name,
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

			updatedAMetaDescription, err := migrationManager.Apply(migrationDescription, false, false)

			Expect(err).To(BeNil())

			Expect(updatedAMetaDescription.FindField("b_set")).To(BeNil())
			Expect(updatedAMetaDescription.FindField("explicitly_set_b_set")).NotTo(BeNil())
		})

		Context("having object B", func() {
			var bMetaDescription *meta.Meta
			BeforeEach(func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				bMetaDescription = object.GetBaseMetaData(utils.RandomString(8))
				bMetaObj, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(bMetaDescription)
				Expect(err).To(BeNil())

				err = metaStore.Create(bMetaObj)
				Expect(err).To(BeNil())

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())
			})

			It("adds a reverse outer link when a new inner field is being added to an object", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

				field := meta.Field{
					Name:     "target_object",
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

				_, err = migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				aMetaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(aMetaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name))).NotTo(BeNil())
				Expect(aMetaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name)).LinkMeta.Name).To(Equal(bMetaDescription.Name))

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())

			})

			It("renames reverse outer links if object which owns inner link is being renamed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

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

				updatedBMetaDescription, err := migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				renamedBMetaDescription := updatedBMetaDescription.Clone()
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

				_, err = migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())

				aMetaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())

				Expect(aMetaObj.FindField("bb_set")).NotTo(BeNil())
				Expect(aMetaObj.FindField("bb_set").LinkMeta.Name).To(Equal("bb"))
			})
			//
			It("removes outer links if object which owns inner link is being deleted", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

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

				_, err = migrationManager.Apply(migrationDescription, false, false)
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

				_, err = migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())

				metaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())

				Expect(metaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name))).To(BeNil())

			})

			It("removes outer links if inner link is being removed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction()
				Expect(err).To(BeNil())

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

				_, err = migrationManager.Apply(migrationDescription, false, false)
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

				_, err = migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())

				metaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(metaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name))).To(BeNil())
			})
		})
	})
})
