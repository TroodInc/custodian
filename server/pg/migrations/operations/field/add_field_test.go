package field

import (
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/pg"
	"custodian/server/pg/migrations/operations/object"
	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions"
	"custodian/server/transactions/file_transaction"
	"custodian/utils"
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'AddField' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer.Remove, metaDescriptionSyncer.Create)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	var metaDescription *description.MetaDescription

	flushDb := func() {
		//Flush meta/database
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	}

	//setup transaction
	AfterEach(flushDb)

	//setup MetaDescription
	BeforeEach(func() {
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
			},
		}
	})

	It("creates column for specified table in the database", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		operation := object.NewCreateObjectOperation(metaDescription)

		metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		err = operation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		//

		field := description.Field{Name: "new_field", Type: description.FieldTypeString, Optional: true}
		fieldOperation := NewAddFieldOperation(&field)

		err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
		//
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())
		Expect(metaDdlFromDB.Columns).To(HaveLen(2))
		Expect(metaDdlFromDB.Columns[1].Optional).To(BeTrue())
		Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(description.FieldTypeString))
		Expect(metaDdlFromDB.Columns[1].Name).To(Equal("new_field"))

		globalTransactionManager.CommitTransaction(globalTransaction)
	})
	
	It("creates enum", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		operation := object.NewCreateObjectOperation(metaDescription)

		metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		err = operation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		//
		enums := description.EnumChoices{"string", "ping", "CAMEL"}
		field := description.Field{Name: "myEnum", Type: description.FieldTypeEnum, Optional: true, Enum: enums}

		fieldOperation := NewAddFieldOperation(&field)

		err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
		//
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
		Expect(metaDescription.Name).To(Equal("a"))
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())
		Expect(metaDdlFromDB.Columns).To(HaveLen(2))
		Expect(metaDdlFromDB.Columns[1].Optional).To(BeTrue())
		Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(description.FieldTypeEnum))
		Expect(metaDdlFromDB.Columns[1].Enum).To(HaveLen(3))
		Expect(metaDdlFromDB.Columns[1].Enum[2]).To(Equal("CAMEL"))

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("creates sequence for specified column in the database", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		operation := object.NewCreateObjectOperation(metaDescription)

		metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		//sync MetaDescription with DB

		//
		field := description.Field{
			Name:     "new_field",
			Type:     description.FieldTypeNumber,
			Optional: true,
			Def: map[string]interface{}{
				"func": "nextval",
			},
		}

		fieldOperation := NewAddFieldOperation(&field)
		err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
		//
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())
		Expect(metaDdlFromDB.Seqs).To(HaveLen(2))
		Expect(metaDdlFromDB.Seqs[1].Name).To(Equal("o_a_new_field_seq"))

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("creates constraint for specified column in the database", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		operation := object.NewCreateObjectOperation(metaDescription)
		metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		//create linked MetaDescription obj
		linkedMetaDescription := &description.MetaDescription{
			Name: "b",
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
			},
		}

		linkedMetaOperation := object.NewCreateObjectOperation(linkedMetaDescription)
		linkedMetaDescription, err = linkedMetaOperation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		err = linkedMetaOperation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		//Run field operations
		field := description.Field{
			Name:     "link_to_a",
			Type:     description.FieldTypeObject,
			LinkType: description.LinkTypeInner,
			LinkMeta: linkedMetaDescription.Name,
			Optional: false,
			OnDelete: description.OnDeleteCascadeVerbose,
		}

		fieldOperation := NewAddFieldOperation(&field)

		err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		//Check constraint
		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
		//
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())
		Expect(metaDdlFromDB.IFKs).To(HaveLen(1))
		Expect(metaDdlFromDB.IFKs[0].ToTable).To(Equal(pg.GetTableName(linkedMetaDescription.Name)))
		Expect(metaDdlFromDB.IFKs[0].ToColumn).To(Equal(linkedMetaDescription.Key))
		Expect(metaDdlFromDB.IFKs[0].FromColumn).To(Equal("link_to_a"))
		Expect(metaDdlFromDB.IFKs[0].OnDelete).To(Equal(description.OnDeleteCascadeDb))

		globalTransactionManager.CommitTransaction(globalTransaction)
	})
})
