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
	"github.com/getlantern/deepcopy"
	"server/migrations/operations/field"
)

var _ = FDescribe("'AddField' Migration Operation", func() {
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
	var fieldToUpdate description.Field

	BeforeEach(func() {
		//Flush meta/database
		var err error
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	Describe("'Direct' case for simple field", func() {
		//setup MetaObj
		BeforeEach(func() {
			//"Direct" case
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
						Name: "number",
						Type: description.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
				},
			}
			//create Meta
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			operation := object.NewCreateObjectOperation(metaDescription)
			//sync Meta
			metaObj, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			//

			// clone a field
			field := metaObj.FindField("number").Field
			err = deepcopy.Copy(&fieldToUpdate, *field)
			Expect(err).To(BeNil())
			Expect(fieldToUpdate).NotTo(BeNil())

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("changes column type", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Type = description.FieldTypeString
			newFieldDescription, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryFieldDescription(fieldToUpdate, metaObj)
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaObj.FindField("number"), newFieldDescription)
			err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(pg.ColumnTypeText))

			globalTransactionManager.RollbackTransaction(globalTransaction)
		})

		It("changes nullability flag", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Optional = false
			newFieldDescription, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryFieldDescription(fieldToUpdate, metaObj)
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaObj.FindField("number"), newFieldDescription)
			err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Optional).To(BeFalse())

			globalTransactionManager.RollbackTransaction(globalTransaction)
		})

		It("changes name", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Name = "updated-name"
			newFieldDescription, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryFieldDescription(fieldToUpdate, metaObj)
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaObj.FindField("number"), newFieldDescription)
			err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Name).To(Equal("updated-name"))

			globalTransactionManager.RollbackTransaction(globalTransaction)
		})

		It("drops default value", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Def = ""
			newFieldDescription, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryFieldDescription(fieldToUpdate, metaObj)
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaObj.FindField("number"), newFieldDescription)
			err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			//check that field`s default value has been dropped
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Defval).To(Equal(""))
			//check sequence has been dropped
			Expect(metaDdlFromDB.Seqs).To(HaveLen(1))

			globalTransactionManager.RollbackTransaction(globalTransaction)
		})

		It("does all things described above at once", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
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

		It("creates default value and sequence", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			//prepare field, set its default value to nil
			fieldToUpdate.Def = ""
			newFieldDescription, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryFieldDescription(fieldToUpdate, metaObj)
			Expect(err).To(BeNil())

			fieldOperation := NewUpdateFieldOperation(metaObj.FindField("number"), newFieldDescription)
			err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			metaFieldOperation := field.NewUpdateFieldOperation(metaObj.FindField("number"), newFieldDescription)
			metaObj, err = metaFieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//

			//modify field
			fieldToUpdate.Def = map[string]interface{}{
				"func": "nextval",
			}
			newFieldDescription, err = meta.NewMetaFactory(metaDescriptionSyncer).FactoryFieldDescription(fieldToUpdate, metaObj)
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation = NewUpdateFieldOperation(metaObj.FindField("number"), newFieldDescription)
			err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())

			//check that field`s default value has been dropped
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Defval).To(Equal("nextval('o_a_number_seq'::regclass)"))
			//check sequence has been dropped
			Expect(metaDdlFromDB.Seqs).To(HaveLen(2))

			globalTransactionManager.RollbackTransaction(globalTransaction)
		})
	})

})
