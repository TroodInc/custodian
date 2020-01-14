package migrations_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"utils"
	"server/object/meta"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/object/description"
	"server/pg/migrations/operations/object"
	migrations_description "server/migrations/description"
	. "server/migrations/migrations"
	"server/pg/migrations/managers"
)

var _ = Describe("Automated generic links` migrations` spawning", func() {
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
	migrationManager := managers.NewMigrationManager(
		metaStore, migrationStore, dataManager, globalTransactionManager,
	)
	var metaDescription *description.MetaDescription

	BeforeEach(func() {
		err := metaStore.Flush()
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
		//create MetaDescription
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		//sync MetaDescription

		operation := object.NewCreateObjectOperation(metaDescription)

		metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		//sync DB
		err = operation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		//
		err = globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
	})

	Describe("Automated links` migrations` spawning", func() {
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

			migrationDescription := migrations_description.GetObjectCreationMigration(
				"random", "", nil, bMetaDescription,
			)

			migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)

			Expect(err).To(BeNil())
			Expect(migration.RunAfter).To(HaveLen(1))
			Expect(migration.RunAfter[0].Operations).To(HaveLen(1))
			Expect(migration.RunAfter[0].Operations[0].Field.Name).To(Equal(meta.ReverseInnerLinkName("b")))
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

			migrationDescription := migrations_description.GetObjectCreationMigration(
				"random", "", nil, bMetaDescription,
			)

			_, err := migrationManager.Apply(migrationDescription, false, false)
			Expect(err).To(BeNil())

			aMetaDescription, _, err := metaDescriptionSyncer.Get("a")
			Expect(err).To(BeNil())
			Expect(aMetaDescription.FindField("b_set")).NotTo(BeNil())

			migrationDescription = migrations_description.GetFieldCreationMigration(
				"random", "a", nil, description.Field{
				Name:           "explicitly_set_b_set",
				Type:           description.FieldTypeArray,
				LinkType:       description.LinkTypeOuter,
				OuterLinkField: "a",
				LinkMeta:       "b",
			})

			migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)
			Expect(err).To(BeNil())

			Expect(migration.RunBefore).To(HaveLen(1))
			Expect(migration.RunBefore[0].Operations[0].Type).To(Equal(migrations_description.RemoveFieldOperation))
			Expect(migration.RunBefore[0].Operations[0].Field.Name).To(Equal(meta.ReverseInnerLinkName("b")))
		})

		Context("having object B", func() {
			var bMetaDescription *description.MetaDescription

			BeforeEach(func() {
				bMetaDescription = description.GetBasicMetaDescription("random")
				bMetaObj, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(bMetaDescription)
				Expect(err).To(BeNil())

				err = metaStore.Create(bMetaObj)
				Expect(err).To(BeNil())
			})

			It("adds a reverse outer link when a new inner link field is being added to an object", func() {
				field := description.Field{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				}

				migrationDescription := migrations_description.GetFieldCreationMigration(
					"random", bMetaDescription.Name, nil, field,
				)

				migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)

				Expect(err).To(BeNil())
				Expect(migration.RunAfter).To(HaveLen(1))
				Expect(migration.RunAfter[0].Operations).To(HaveLen(1))
				Expect(migration.RunAfter[0].Operations[0].Field.Name).To(Equal(meta.ReverseInnerLinkName(bMetaDescription.Name)))
			})

			It("renames reverse outer link if object which owns inner link is being renamed", func() {
				field := description.Field{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				}

				migrationDescription := migrations_description.GetFieldCreationMigration(
					"random", bMetaDescription.Name, nil, field,
				)

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
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				field := description.Field{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				}

				migrationDescription := migrations_description.GetFieldCreationMigration(
					"random", bMetaDescription.Name, nil, field,
				)

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

				migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)
				Expect(err).To(BeNil())

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())

				Expect(migration.RunBefore).To(HaveLen(1))
				Expect(migration.RunBefore[0].Operations).To(HaveLen(1))
				Expect(migration.RunBefore[0].Operations[0].Type).To(Equal(migrations_description.RemoveFieldOperation))
				Expect(migration.RunBefore[0].Operations[0].Field.Name).To(Equal(meta.ReverseInnerLinkName(bMetaDescription.Name)))
			})

			It("removes outer links if inner link is being removed", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())

				field := description.Field{
					Name:     "a",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					Optional: false,
				}

				migrationDescription := migrations_description.GetFieldCreationMigration(
					"random", bMetaDescription.Name, nil, field,
				)

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

				migration, err := NewMigrationFactory(metaDescriptionSyncer).FactoryForward(migrationDescription)
				Expect(err).To(BeNil())

				err = globalTransactionManager.CommitTransaction(globalTransaction)
				Expect(err).To(BeNil())

				Expect(migration.RunBefore).To(HaveLen(1))
				Expect(migration.RunBefore[0].Operations).To(HaveLen(1))
				Expect(migration.RunBefore[0].Operations[0].Type).To(Equal(migrations_description.RemoveFieldOperation))
				Expect(migration.RunBefore[0].Operations[0].Field.Name).To(Equal(meta.ReverseInnerLinkName(bMetaDescription.Name)))
			})
		})
	})
})
