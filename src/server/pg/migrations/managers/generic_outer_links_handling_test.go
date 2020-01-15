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

var _ = Describe("Generic outer links spawned migrations appliance", func() {
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

	Describe("Spawned migrations` appliance", func() {
		XIt("adds reverse generic outer link while object is being created", func() {
			bMetaDescription := description.NewMetaDescription(
				"b_ellyac",
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
						LinkMetaList: []string{"a"},
						Optional:     false,
					},
				},
				nil,
				false,
			)

			migrationDescription := &migrations_description.MigrationDescription{
				Id:        "create-b-meta-for-reverse-generic-link-test",
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
			Expect(aMetaObj.FindField(meta.ReverseInnerLinkName("b")).LinkMeta.Name).To(Equal("b_ellyac"))
		})

		Context("having object B", func() {
			var bMetaDescription *description.MetaDescription
			BeforeEach(func() {
				bMetaDescription = description.GetBasicMetaDescription("random")

				bMetaObj, err := metaStore.NewMeta(bMetaDescription)
				Expect(err).To(BeNil())
				err = metaStore.Create(bMetaObj)
				Expect(err).To(BeNil())
			})

			It("adds a reverse generic outer link when a new inner generic field is being added to an object", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{"a"},
					Optional:     false,
				}

				migrationDescription := migrations_description.GetFieldCreationMigration(
					"random", bMetaDescription.Name, nil, field,
				)

				_, err := migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				aMetaObj, _, err := metaStore.Get("a", false)
				Expect(err).To(BeNil())
				Expect(aMetaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name))).NotTo(BeNil())
				Expect(aMetaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name)).LinkMeta.Name).To(Equal(bMetaDescription.Name))
			})

			It("removes and adds reverse generic outer links while inner generic field`s LinkMetaList is being updated", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{"a"},
					Optional:     false,
				}

				migrationDescription := migrations_description.GetFieldCreationMigration(
					"random", bMetaDescription.Name, nil, field,
				)

				_, err := migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				cMetaObj, err := metaStore.NewMeta(description.GetBasicMetaDescription("random"))
				Expect(err).To(BeNil())
				err = metaStore.Create(cMetaObj)
				Expect(err).To(BeNil())

				//LinkMetaList is being changed
				field = description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{cMetaObj.Name},
					Optional:     false,
				}

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "cfiv48",
					ApplyTo:   bMetaDescription.Name,
					DependsOn: nil,
					Operations: [] migrations_description.MigrationOperationDescription{
						{
							Type:  migrations_description.UpdateFieldOperation,
							Field: &migrations_description.MigrationFieldDescription{Field: field, PreviousName: field.Name},
						},
					},
				}

				_, err = migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				aMetaObj, _, err := metaStore.Get("a", false)
				Expect(err).To(BeNil())
				Expect(aMetaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name))).To(BeNil())

				cMetaObj, _, err = metaStore.Get(cMetaObj.Name, false)
				Expect(err).To(BeNil())
				Expect(cMetaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name))).NotTo(BeNil())
				Expect(cMetaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name)).LinkMeta.Name).To(Equal(bMetaDescription.Name))

			})

			It("renames reverse generic outer links if object which owns inner generic link is being renamed", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{"a"},
					Optional:     false,
				}

				migrationDescription := migrations_description.GetFieldCreationMigration(
					"random", bMetaDescription.Name, nil, field,
				)

				updatedBMetaDescription, err := migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				renamedBMetaDescription := updatedBMetaDescription.Clone()
				renamedBMetaDescription.Name = "bb"

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "bmrnmr",
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

			It("removes generic outer links if object which owns inner generic link is being deleted", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{"a"},
					Optional:     false,
				}

				migrationDescription := migrations_description.GetFieldCreationMigration(
					"random", bMetaDescription.Name, nil, field,
				)

				_, err := migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "sbhjvl",
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

				Expect(metaObj.FindField(meta.ReverseInnerLinkName(bMetaDescription.Name))).To(BeNil())
			})

			It("removes generic outer links if inner generic link is being removed", func() {
				field := description.Field{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{"a"},
					Optional:     false,
				}

				migrationDescription := migrations_description.GetFieldCreationMigration(
					"random", bMetaDescription.Name, nil, field,
				)

				_, err := migrationManager.Apply(migrationDescription, true, false)
				Expect(err).To(BeNil())

				migrationDescription = &migrations_description.MigrationDescription{
					Id:        "5o0pvu",
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
