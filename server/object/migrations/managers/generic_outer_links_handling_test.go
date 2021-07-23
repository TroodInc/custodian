package managers

import (
	migrations_description "custodian/server/migrations/description"
	obj "custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Generic outer links spawned migrations appliance", func() {
	appConfig := utils.GetConfig()
	db, _ := obj.NewDbConnection(appConfig.DbConnectionUrl)
	dbTransactionManager := obj.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := obj.NewPgMetaDescriptionSyncer(dbTransactionManager, obj.NewCache())

	metaStore := obj.NewStore(metaDescriptionSyncer, dbTransactionManager)

	migrationManager := NewMigrationManager(metaDescriptionSyncer, dbTransactionManager)

	var metaDescription *description.MetaDescription

	flushDb := func() {
		//Flush meta/database
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	}

	AfterEach(flushDb)

	//setup MetaDescription
	JustBeforeEach(func() {
		metaDescription = obj.GetBaseMetaData(utils.RandomString(8))

		metaObj, err := metaStore.MetaDescriptionSyncer.Cache().FactoryMeta(metaDescription)
		Expect(err).To(BeNil())

		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
	})

	Describe("Spawned migrations` appliance", func() {
		It("adds reverse generic outer link while object is being created", func() {
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
						Name:         "target_object",
						Type:         description.FieldTypeGeneric,
						LinkType:     description.LinkTypeInner,
						LinkMetaList: []string{metaDescription.Name},
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

			_, err := migrationManager.Apply(migrationDescription, false, false)
			Expect(err).To(BeNil())

			aMetaObj, _, err := metaStore.Get(metaDescription.Name, false)
			Expect(aMetaObj.FindField(obj.ReverseInnerLinkName("b"))).NotTo(BeNil())
			Expect(aMetaObj.FindField(obj.ReverseInnerLinkName("b")).LinkMeta.Name).To(Equal("b"))

		})

		Context("having object B", func() {
			var bMetaDescription *description.MetaDescription
			BeforeEach(func() {
				bMetaDescription = obj.GetBaseMetaData(utils.RandomString(8))
				bMetaObj, err := metaStore.MetaDescriptionSyncer.Cache().FactoryMeta(bMetaDescription)
				Expect(err).To(BeNil())

				err = metaStore.Create(bMetaObj)
				Expect(err).To(BeNil())
			})

			It("adds a reverse generic outer link when a new inner generic field is being added to an object", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{metaDescription.Name},
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

				aMetaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(aMetaObj.FindField(obj.ReverseInnerLinkName(bMetaDescription.Name))).NotTo(BeNil())
				Expect(aMetaObj.FindField(obj.ReverseInnerLinkName(bMetaDescription.Name)).LinkMeta.Name).To(Equal(bMetaDescription.Name))
			})

			It("removes and adds reverse generic outer links while inner generic field`s LinkMetaList is being updated", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{metaDescription.Name},
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

				cMetaDescription := obj.GetBaseMetaData(utils.RandomString(8))
				cMetaObj, err := metaStore.MetaDescriptionSyncer.Cache().FactoryMeta(cMetaDescription)
				Expect(err).To(BeNil())

				err = metaStore.Create(cMetaObj)
				Expect(err).To(BeNil())

				//LinkMetaList is being changed
				field = description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{cMetaDescription.Name},
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

				_, err = migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				aMetaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(aMetaObj.FindField(obj.ReverseInnerLinkName(bMetaDescription.Name))).To(BeNil())

				cMetaObj, _, err = metaStore.Get(cMetaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(cMetaObj.FindField(obj.ReverseInnerLinkName(bMetaDescription.Name))).NotTo(BeNil())
				Expect(cMetaObj.FindField(obj.ReverseInnerLinkName(bMetaDescription.Name)).LinkMeta.Name).To(Equal(bMetaDescription.Name))

			})

			It("renames reverse generic outer links if object which owns inner generic link is being renamed", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{metaDescription.Name},
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
			It("removes generic outer links if object which owns inner generic link is being deleted", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{metaDescription.Name},
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

				_, err = migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				metaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())

				Expect(metaObj.FindField(obj.ReverseInnerLinkName(bMetaDescription.Name))).To(BeNil())

			})

			It("removes generic outer links if inner generic link is being removed", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{metaDescription.Name},
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

				_, err = migrationManager.Apply(migrationDescription, false, false)
				Expect(err).To(BeNil())

				metaObj, _, err := metaStore.Get(metaDescription.Name, false)
				Expect(err).To(BeNil())
				Expect(metaObj.FindField(obj.ReverseInnerLinkName(bMetaDescription.Name))).To(BeNil())
			})
		})
	})
})
