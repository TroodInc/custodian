package field

import (
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/description"
	"server/object/meta"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/transactions/file_transaction"
	"utils"
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

	//setup MetaDescription
	BeforeEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())

		metaDescription = description.GetBasicMetaDescription("random")
		MetaObj, err := metaStore.NewMeta(metaDescription)
		err = metaStore.Create(MetaObj)
		Expect(err).To(BeNil())
	})

	It("creates column for specified table in the database", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)

		field := description.Field{Name: "new_field", Type: description.FieldTypeString, Optional: true}
		fieldOperation := NewAddFieldOperation(&field)

		err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
		//
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
		globalTransactionManager.CommitTransaction(globalTransaction)

		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())
		Expect(metaDdlFromDB.Columns).To(HaveLen(2))
		Expect(metaDdlFromDB.Columns[1].Optional).To(BeTrue())
		Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(description.FieldTypeString))
		Expect(metaDdlFromDB.Columns[1].Name).To(Equal("new_field"))
	})

	It("creates sequence for specified column in the database", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
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
		globalTransactionManager.CommitTransaction(globalTransaction)

		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())
		Expect(metaDdlFromDB.Seqs).To(HaveLen(2))
		Expect(metaDdlFromDB.Seqs[1].Name).To(Equal("o_" + metaDescription.Name + "_new_field_seq"))
	})

	It("creates constraint for specified column in the database", func() {
		//create linked MetaDescription obj
		linkedMetaDescription := description.GetBasicMetaDescription("random")

		MetaObj, err := metaStore.NewMeta(linkedMetaDescription)
		err = metaStore.Create(MetaObj)
		Expect(err).To(BeNil())

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
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
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
		globalTransactionManager.CommitTransaction(globalTransaction)

		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())
		Expect(metaDdlFromDB.IFKs).To(HaveLen(1))
		Expect(metaDdlFromDB.IFKs[0].ToTable).To(Equal(pg.GetTableName(linkedMetaDescription.Name)))
		Expect(metaDdlFromDB.IFKs[0].ToColumn).To(Equal(linkedMetaDescription.Key))
		Expect(metaDdlFromDB.IFKs[0].FromColumn).To(Equal("link_to_a"))
		Expect(metaDdlFromDB.IFKs[0].OnDelete).To(Equal(description.OnDeleteCascadeDb))

	})
})
