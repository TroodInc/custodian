package field

import (
	"database/sql"
	"server/object/meta"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"server/pg"
	"server/pg/migrations/operations/object"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("'AddField' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewMetaStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	var metaDescription *meta.Meta

	flushDb := func() {
		//Flush meta/database
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	}

	//setup transaction
	AfterEach(flushDb)

	//setup MetaDescription
	BeforeEach(func() {
		metaDescription = &meta.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
	})

	It("creates column for specified table in the database", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		operation := object.NewCreateObjectOperation(metaDescription)

		metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		err = operation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		//

		field := meta.Field{Name: "new_field", Type: meta.FieldTypeString, Optional: true}
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
		Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(meta.FieldTypeString))
		Expect(metaDdlFromDB.Columns[1].Name).To(Equal("new_field"))

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("creates sequence for specified column in the database", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		operation := object.NewCreateObjectOperation(metaDescription)

		metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		//sync MetaDescription with DB

		//
		field := meta.Field{
			Name:     "new_field",
			Type:     meta.FieldTypeNumber,
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
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		operation := object.NewCreateObjectOperation(metaDescription)
		metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		//create linked MetaDescription obj
		linkedMetaDescription := &meta.Meta{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []*meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
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
		field := meta.Field{
			Name:     "link_to_a",
			Type:     meta.FieldTypeObject,
			LinkType: meta.LinkTypeInner,
			LinkMeta: linkedMetaDescription.Name,
			Optional: false,
			OnDelete: meta.OnDeleteCascadeVerbose,
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
		Expect(metaDdlFromDB.IFKs[0].OnDelete).To(Equal(meta.OnDeleteCascadeDb))

		globalTransactionManager.CommitTransaction(globalTransaction)
	})
})
