package managers

import (
	migrations_description "custodian/server/migrations/description"
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Outer links spawned migrations appliance", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache(), db)

	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)
	migrationManager := NewMigrationManager(
		metaDescriptionSyncer, dbTransactionManager, db,
	)

	var metaDescription *description.MetaDescription

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
		metaObj, err := metaStore.MetaDescriptionSyncer.Cache().FactoryMeta(metaDescription)
		Expect(err).To(BeNil())

		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
	})

	Describe("Spawned migrations` appliance", func() {
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
						LinkMeta: metaDescription.Name,
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
				Operations: []migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: bMetaDescription,
					},
				},
			}

			_, err := migrationManager.Apply(migrationDescription, false, false)
			Expect(err).To(BeNil())

			aMetaObj, _, err := metaStore.Get(metaDescription.Name, false)
			Expect(aMetaObj.FindField(object.ReverseInnerLinkName("b"))).NotTo(BeNil())
			Expect(aMetaObj.FindField(object.ReverseInnerLinkName("b")).LinkMeta.Name).To(Equal("b"))
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
						LinkMeta: metaDescription.Name,
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
				Operations: []migrations_description.MigrationOperationDescription{
					{
						Type:            migrations_description.CreateObjectOperation,
						MetaDescription: bMetaDescription,
					},
				},
			}

			_, err := migrationManager.Apply(migrationDescription, false, false)
			Expect(err).To(BeNil())

			aMetaDescription, _, err := metaDescriptionSyncer.Get(metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(aMetaDescription.FindField("b_set")).NotTo(BeNil())

			migrationDescription = &migrations_description.MigrationDescription{
				Id:        "some-unique-id",
				ApplyTo:   aMetaDescription.Name,
				DependsOn: nil,
				Operations: []migrations_description.MigrationOperationDescription{
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

			updatedAMetaDescription, err := migrationManager.Apply(migrationDescription, false, false)

			Expect(err).To(BeNil())

			Expect(updatedAMetaDescription.FindField("b_set")).To(BeNil())
			Expect(updatedAMetaDescription.FindField("explicitly_set_b_set")).NotTo(BeNil())
		})

		Context("having object B", func() {
			var bMetaDescription *description.MetaDescription
			BeforeEach(func() {

				bMetaDescription = object.GetBaseMetaData(utils.RandomString(8))
				bMetaObj, err := metaStore.MetaDescriptionSyncer.Cache().FactoryMeta(bMetaDescription)
				Expect(err).To(BeNil())

				err = metaStore.Create(bMetaObj)
				Expect(err).To(BeNil())

			})

			It("adds a reverse outer link when a new inner field is being added to an object", func() {

				field := description.Field{
					Name:     "target_object",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: metaDescription.Name,
					Optional: false,
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

				aMetaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(aMetaObj.FindField(object.ReverseInnerLinkName(bMetaDescription.Name))).NotTo(BeNil())
				Expect(aMetaObj.FindField(object.ReverseInnerLinkName(bMetaDescription.Name)).LinkMeta.Name).To(Equal(bMetaDescription.Name))

			})

			It("renames reverse outer links if object which owns inner link is being renamed", func() {

				field := description.Field{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: metaDescription.Name,
					Optional: false,
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

				updatedBMetaDescription, err := migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				renamedBMetaDescription := updatedBMetaDescription.Clone()
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

				_, err = migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				aMetaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())

				Expect(aMetaObj.FindField("bb_set")).NotTo(BeNil())
				Expect(aMetaObj.FindField("bb_set").LinkMeta.Name).To(Equal("bb"))
			})
			//
			It("removes outer links if object which owns inner link is being deleted", func() {

				field := description.Field{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: metaDescription.Name,
					Optional: false,
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

				_, err = migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				metaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())

				Expect(metaObj.FindField(object.ReverseInnerLinkName(bMetaDescription.Name))).To(BeNil())

			})

			It("removes outer links if inner link is being removed", func() {

				field := description.Field{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: metaDescription.Name,
					Optional: false,
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

				_, err = migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				metaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(metaObj.FindField(object.ReverseInnerLinkName(bMetaDescription.Name))).To(BeNil())
			})
		})
	})
})
