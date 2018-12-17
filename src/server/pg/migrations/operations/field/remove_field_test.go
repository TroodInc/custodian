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

	//setup transaction
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
			operation := object.NewCreateObjectOperation(metaDescription)
			//sync Meta
			metaObj, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			//
			globalTransactionManager.CommitTransaction(globalTransaction)

		})

		It("drops column", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewRemoveFieldOperation(metaObj.FindField("number"))
			err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field has been removed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns).To(HaveLen(1))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("drops sequence", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewRemoveFieldOperation(metaObj.FindField("number"))
			err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s default value has been dropped
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			//check sequence has been dropped
			Expect(metaDdlFromDB.Seqs).To(HaveLen(1))

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
			operation := object.NewCreateObjectOperation(bMetaDescription)
			//sync Meta & DB
			bMetaObj, err := operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			err = operation.SyncDbDescription(bMetaObj, globalTransaction.DbTransaction)
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
			operation = object.NewCreateObjectOperation(metaDescription)
			//sync Meta
			metaObj, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			//

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("Drops IFK if field is being dropped", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewRemoveFieldOperation(metaObj.FindField("b"))
			err = fieldOperation.SyncDbDescription(metaObj, globalTransaction.DbTransaction)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaObj, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that IFK has been dropped
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			globalTransactionManager.CommitTransaction(globalTransaction)

			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.IFKs).To(HaveLen(0))

		})
	})
})
