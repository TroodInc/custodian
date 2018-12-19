package migrations

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
	"server/pg/migrations/operations/field"
	migrations_description "server/migrations/description"
)

var _ = Describe("Migration Factory", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer.Remove, metaDescriptionSyncer.Create)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	var metaDescription *description.MetaDescription
	var metaObj *meta.Meta

	flushDb := func() {
		//Flush meta/database
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		err = metaStore.Flush(globalTransaction)
		Expect(err).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)
	}

	BeforeEach(flushDb)
	AfterEach(flushDb)

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
		//create Meta
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		//sync Meta
		metaObj, err = meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(metaDescription)
		Expect(err).To(BeNil())

		operation := object.NewCreateObjectOperation(metaObj)

		metaObj, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		//sync DB
		err = operation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
		Expect(err).To(BeNil())
		//
		err = globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(err).To(BeNil())
	})

	It("factories migration containing CreateObjectOperation", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   "",
			DependsOn: nil,
			Operations: [] migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.CreateObjectOperation,
					MetaDescription: *metaDescription,
				},
			},
		}

		migration, err := NewMigrationFactory(metaStore, globalTransaction, metaDescriptionSyncer).Factory(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*object.CreateObjectOperation)
		Expect(ok).To(BeTrue())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("factories migration containing RenameObjectOperation", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		updatedMetaDescription := metaDescription.Clone()
		updatedMetaDescription.Name = "updatedA"

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   "a",
			DependsOn: nil,
			Operations: [] migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.RenameObjectOperation,
					MetaDescription: *updatedMetaDescription,
				},
			},
		}

		migration, err := NewMigrationFactory(metaStore, globalTransaction, metaDescriptionSyncer).Factory(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*object.RenameObjectOperation)
		Expect(ok).To(BeTrue())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("factories migration containing RenameObjectOperation", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   "a",
			DependsOn: nil,
			Operations: [] migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.DeleteObjectOperation,
					MetaDescription: *metaDescription,
				},
			},
		}

		migration, err := NewMigrationFactory(metaStore, globalTransaction, metaDescriptionSyncer).Factory(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*object.DeleteObjectOperation)
		Expect(ok).To(BeTrue())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("factories migration containing AddFieldOperation", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   "a",
			DependsOn: nil,
			Operations: [] migrations_description.MigrationOperationDescription{
				{
					Type:  migrations_description.AddFieldOperation,
					Field: migrations_description.MigrationFieldDescription{Field: description.Field{Name: "new-field", Type: description.FieldTypeString, Optional: true}},
				},
			},
		}

		migration, err := NewMigrationFactory(metaStore, globalTransaction, metaDescriptionSyncer).Factory(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*field.AddFieldOperation)
		Expect(ok).To(BeTrue())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("factories migration containing RemoveFieldOperation", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   "a",
			DependsOn: nil,
			Operations: [] migrations_description.MigrationOperationDescription{
				{
					Type:  migrations_description.RemoveFieldOperation,
					Field: migrations_description.MigrationFieldDescription{Field: description.Field{Name: "date"}},
				},
			},
		}

		migration, err := NewMigrationFactory(metaStore, globalTransaction, metaDescriptionSyncer).Factory(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*field.RemoveFieldOperation)
		Expect(ok).To(BeTrue())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("factories migration containing UpdateFieldOperation", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   "a",
			DependsOn: nil,
			Operations: [] migrations_description.MigrationOperationDescription{
				{
					Type:  migrations_description.RemoveFieldOperation,
					Field: migrations_description.MigrationFieldDescription{Field: description.Field{Name: "date"}},
				},
			},
		}

		migration, err := NewMigrationFactory(metaStore, globalTransaction, metaDescriptionSyncer).Factory(migrationDescription)

		Expect(err).To(BeNil())
		Expect(migration.Operations).To(HaveLen(1))

		_, ok := migration.Operations[0].(*field.RemoveFieldOperation)
		Expect(ok).To(BeTrue())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})
})
