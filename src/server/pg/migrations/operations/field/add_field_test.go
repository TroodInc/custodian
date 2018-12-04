package field

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
	"database/sql"
)

var _ = Describe("'AddField' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaDescriptionSyncer := meta.NewFileMetaDescriptionSyncer("./")
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer.Remove, metaDescriptionSyncer.Create)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	var metaDescription description.MetaDescription

	//setup transaction
	BeforeEach(func() {
		var err error

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	//setup MetaDescription
	BeforeEach(func() {
		metaDescription = description.MetaDescription{
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

	//setup teardown
	AfterEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("creates column for specified table in the database", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		//create Meta
		operation := object.NewCreateObjectOperation(&metaDescription)
		metaObj, err := operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		//sync Meta with DB
		err = operation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		//
		field := description.Field{Name: "new_field", Type: description.FieldTypeString, Optional: true}
		fieldDescription, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryFieldDescription(field, metaObj)
		Expect(err).To(BeNil())
		fieldOperation := NewAddFieldOperation(fieldDescription)

		fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
		//
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())
		Expect(metaDdlFromDB.Columns).To(HaveLen(2))
		Expect(metaDdlFromDB.Columns[1].Optional).To(BeTrue())
		Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(pg.ColumnTypeText))
		Expect(metaDdlFromDB.Columns[1].Name).To(Equal("new_field"))

		globalTransactionManager.RollbackTransaction(globalTransaction)
	})

	It("creates sequence for specified column in the database", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		//create Meta
		operation := object.NewCreateObjectOperation(&metaDescription)
		metaObj, err := operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		//sync Meta with DB
		err = operation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		//
		field := description.Field{
			Name:     "new_field",
			Type:     description.FieldTypeNumber,
			Optional: true,
			Def: map[string]interface{}{
				"func": "nextval",
			},
		}

		fieldDescription, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryFieldDescription(field, metaObj)
		fieldOperation := NewAddFieldOperation(fieldDescription)
		fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
		//
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())
		Expect(metaDdlFromDB.Seqs).To(HaveLen(2))
		Expect(metaDdlFromDB.Seqs[1].Name).To(Equal("o_a_new_field_seq"))

		globalTransactionManager.RollbackTransaction(globalTransaction)
	})

	It("creates constraint for specified column in the database", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		//create Meta
		operation := object.NewCreateObjectOperation(&metaDescription)
		metaObj, err := operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = operation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		//create linked Meta obj
		linkedMetaDescription := description.MetaDescription{
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
		linkedMetaOperation := object.NewCreateObjectOperation(&linkedMetaDescription)
		linkedMetaObj, err := linkedMetaOperation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = linkedMetaOperation.SyncDbDescription(linkedMetaObj, globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		//Run field operations
		field := description.Field{
			Name:     "link_to_a",
			Type:     description.FieldTypeObject,
			LinkType: description.LinkTypeInner,
			LinkMeta: linkedMetaObj.Name,
			Optional: false,
			OnDelete: description.OnDeleteCascadeVerbose,
		}

		fieldDescription, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryFieldDescription(field, metaObj)
		Expect(err).To(BeNil())
		fieldOperation := NewAddFieldOperation(fieldDescription)

		err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		//Check constraint
		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
		//
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())
		Expect(metaDdlFromDB.IFKs).To(HaveLen(1))
		Expect(metaDdlFromDB.IFKs[0].ToTable).To(Equal(pg.GetTableName(linkedMetaObj.Name)))
		Expect(metaDdlFromDB.IFKs[0].ToColumn).To(Equal(linkedMetaObj.Key.Name))
		Expect(metaDdlFromDB.IFKs[0].FromColumn).To(Equal("link_to_a"))
		Expect(metaDdlFromDB.IFKs[0].OnDelete).To(Equal(description.OnDeleteCascadeDb))

		globalTransactionManager.RollbackTransaction(globalTransaction)
	})
})
