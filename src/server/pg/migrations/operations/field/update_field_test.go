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
				{
					Name: "number",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
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

	It("alters column for specified table in the database", func() {
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
		field := description.Field{Name: "new-number", Type: description.FieldTypeString, Optional: false, Def: nil}
		newFieldDescription, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryFieldDescription(field, metaObj)
		Expect(err).To(BeNil())

		fieldOperation := NewUpdateFieldOperation(metaObj.FindField("number"), newFieldDescription)

		err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
		Expect(err).To(BeNil())

		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
		//
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())
		Expect(metaDdlFromDB.Columns).To(HaveLen(2))
		//Optional has changed
		Expect(metaDdlFromDB.Columns[1].Optional).To(BeFalse())
		//Type has changed
		Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(pg.ColumnTypeText))
		//Name has changed
		Expect(metaDdlFromDB.Columns[1].Name).To(Equal("new-number"))
		//Default has been dropped
		Expect(metaDdlFromDB.Columns[1].Defval).To(Equal(""))

		globalTransactionManager.RollbackTransaction(globalTransaction)
	})
})
