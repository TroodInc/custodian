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

	var metaDescription *description.MetaDescription
	var metaObj *meta.Meta
	var fieldToUpdate description.Field

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

	Describe("Simple field case", func() {

		//setup MetaObj
		JustBeforeEach(func() {
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
			Expect(err).To(BeNil())

			metaObj, err = meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(metaDescription)
			Expect(err).To(BeNil())

			operation := object.NewCreateObjectOperation(metaObj)
			//sync Meta
			metaObj, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction)
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
			_, err = fieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(pg.ColumnTypeText))

			globalTransactionManager.CommitTransaction(globalTransaction)
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
			_, err = fieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Optional).To(BeFalse())

			globalTransactionManager.CommitTransaction(globalTransaction)
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
			_, err = fieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Name).To(Equal("updated-name"))

			globalTransactionManager.CommitTransaction(globalTransaction)
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
			_, err = fieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s default value has been dropped
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Defval).To(Equal(""))
			//check sequence has been dropped
			Expect(metaDdlFromDB.Seqs).To(HaveLen(1))

			globalTransactionManager.CommitTransaction(globalTransaction)
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
			_, err = fieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
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

			globalTransactionManager.CommitTransaction(globalTransaction)
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
			metaObj, err = fieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
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
			_, err = fieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s default value has been dropped
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Defval).To(Equal("nextval('o_a_number_seq'::regclass)"))
			//check sequence has been dropped
			Expect(metaDdlFromDB.Seqs).To(HaveLen(2))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})
	})

	Describe("Inner FK field case", func() {
		//setup MetaObj
		BeforeEach(func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			//Meta B
			bMetaDescription := &description.MetaDescription{
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
			//sync Meta & DB
			bMetaObj, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(bMetaDescription)
			Expect(err).To(BeNil())

			operation := object.NewCreateObjectOperation(bMetaObj)

			bMetaObj, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			//Meta A
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
						Name:     "b",
						Type:     description.FieldTypeObject,
						LinkMeta: bMetaObj.Name,
						LinkType: description.LinkTypeInner,
					},
				},
			}
			//create Meta
			metaObj, err = meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(metaDescription)
			Expect(err).To(BeNil())

			operation = object.NewCreateObjectOperation(metaObj)

			//sync Meta
			metaObj, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			//

			// clone a field
			field := metaObj.FindField("b").Field
			err = deepcopy.Copy(&fieldToUpdate, *field)
			Expect(err).To(BeNil())
			Expect(fieldToUpdate).NotTo(BeNil())

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("changes IFK name if field is renamed", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Name = "b_link"
			newFieldDescription, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryFieldDescription(fieldToUpdate, metaObj)
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaObj.FindField("b"), newFieldDescription)
			err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Name).To(Equal("b_link"))
			Expect(metaDdlFromDB.IFKs).To(HaveLen(1))
			Expect(metaDdlFromDB.IFKs[0].FromColumn).To(Equal("b_link"))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})
	})
})
