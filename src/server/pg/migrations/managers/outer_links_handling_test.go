package managers

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	migrations_description "server/migrations/description"
	"server/object/description"
	"server/object/meta"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/transactions/file_transaction"
	"utils"
)

var _ = Describe("Outer links spawned migrations appliance", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer.Remove, metaDescriptionSyncer.Create)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	migrationDBDescriptionSyncer := pg.NewDbMetaDescriptionSyncer(dbTransactionManager)
	migrationStore := meta.NewStore(migrationDBDescriptionSyncer, syncer, globalTransactionManager)
	migrationManager := NewMigrationManager(
		metaStore, migrationStore, dataManager, globalTransactionManager,
	)
	var metaDescription *description.MetaDescription

	BeforeEach(func() {
		err := migrationManager.DropHistory()
		Expect(err).To(BeNil())
		//Flush meta/database
		err = metaStore.Flush()
		Expect(err).To(BeNil())
	})

	//setup MetaDescription
	JustBeforeEach(func() {
		metaDescription = &description.MetaDescription{
			Name: "a",
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
		metaObj, err := metaStore.NewMeta(metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
	})

	Context("Spawned migrations` appliance", func() {
		It("adds reverse outer link while object is being created", func() {
			bMetaDescription := description.NewMetaDescription(
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
				Id:        "rxvjgb",
				ApplyTo:   "",
				DependsOn: nil,
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: bMetaDescription,
					},
				},
			}

			_, err := migrationManager.Apply(migrationDescription, true, false)
			Expect(err).To(BeNil())

			aMetaObj, _, err := metaStore.Get("a", false)
			Expect(aMetaObj.FindField(meta.ReverseInnerLinkName("b"))).NotTo(BeNil())
			Expect(aMetaObj.FindField(meta.ReverseInnerLinkName("b")).LinkMeta.Name).To(Equal("b"))
		})

		It("replaces automatically added reverse outer link with explicitly specified new one", func() {
			bMetaDescription := description.NewMetaDescription(
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
				Id:        "jjcw23",
				ApplyTo:   "",
				DependsOn: nil,
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: bMetaDescription,
					},
				},
			}

			_, err := migrationManager.Apply(migrationDescription, true, false)
			Expect(err).To(BeNil())

			aMetaDescription, _, err := metaDescriptionSyncer.Get("a")
			Expect(err).To(BeNil())
			Expect(aMetaDescription.FindField("b_set")).NotTo(BeNil())

			migrationDescription = &migrations_description.MigrationDescription{
				Id:        "dx4zqn",
				ApplyTo:   "a",
				DependsOn: nil,
				Operations: [] migrations_description.MigrationOperationDescription{
					{
						Type: migrations_description.AddFieldOperation,
						Field: &migrations_description.MigrationFieldDescription{
							Field: description.Field{
								Name:           "explicitly_set_b_set",
								Type:           description.FieldTypeArray,
								LinkType:       description.LinkTypeOuter,
								OuterLinkField: "a",
								LinkMeta:       "b",
							},
						},
					},
				},
			}

			updatedAMetaDescription, err := migrationManager.Apply(migrationDescription, true, false)
			Expect(err).To(BeNil())

			Expect(updatedAMetaDescription.FindField("b_set")).To(BeNil())
			Expect(updatedAMetaDescription.FindField("explicitly_set_b_set")).NotTo(BeNil())
		})

		Context("having object B", func() {
			var bMetaDescription *description.MetaDescription
			BeforeEach(func() {
				bMetaDescription = description.NewMetaDescription(
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
					},
					nil,
					false,
				)

				bMetaObj, err := metaStore.NewMeta(bMetaDescription)
				Expect(err).To(BeNil())
				err = metaStore.Create(bMetaObj)
				Expect(err).To(BeNil())
			})

			It("adds a reverse outer link when a new inner field is being added to an object", func() {
				field := description.Field{
					Name:     "target_object",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "00h6s1",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				_, err := migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				aMetaObj, _, err := metaStore.Get("a", false)
				Expect(err).To(BeNil())
				Expect(aMetaObj.FindField(meta.ReverseInnerLinkName("b"))).NotTo(BeNil())
				Expect(aMetaObj.FindField(meta.ReverseInnerLinkName("b")).LinkMeta.Name).To(Equal("b"))
			})

			It("renames reverse outer links if object which owns inner link is being renamed", func() {
				field := description.Field{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "c03pma",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				updatedBMetaDescription, err := migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				renamedBMetaDescription := updatedBMetaDescription.Clone()
				renamedBMetaDescription.Name = "bb"

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "zhbepd",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:            migrations_description.RenameObjectOperation,
							MetaDescription: renamedBMetaDescription,
						},
					},
				}

				_, err = migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				aMetaObj, _, err := metaStore.Get("a", false)
				Expect(err).To(BeNil())

				Expect(aMetaObj.FindField("bb_set")).NotTo(BeNil())
				Expect(aMetaObj.FindField("bb_set").LinkMeta.Name).To(Equal("bb"))
			})

			It("removes outer links if object which owns inner link is being deleted", func() {
				field := description.Field{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "8hmhic",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				_, err := migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "jvq5lk",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:            migrations_description.DeleteObjectOperation,
							MetaDescription: bMetaDescription,
						},
					},
				}

				_, err = migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				metaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())

				Expect(metaObj.FindField(meta.ReverseInnerLinkName("b"))).To(BeNil())
			})

			It("removes outer links if inner link is being removed", func() {
				field := description.Field{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				}

				migrationDescription := &migrations_description.MigrationDescription{
					Id:        "ow7uam",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.AddFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				_, err := migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "zgw7w5",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.RemoveFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field},
						},
					},
				}

				_, err = migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				metaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(metaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name))).To(BeNil())
			})
		})
	})
})
