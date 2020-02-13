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

	Describe("Simple field case", func() {

		//setup MetaObj
		JustBeforeEach(func() {
			//"Direct" case
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
					{
						Name: "number",
						Type: meta.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
				},
			}
			//create MetaDescription
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			operation := object.NewCreateObjectOperation(metaDescription)

			metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//
			globalTransactionManager.CommitTransaction(globalTransaction)

		})

		It("drops column", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewRemoveFieldOperation(metaDescription.FindField("number"))
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field has been removed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns).To(HaveLen(1))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("drops sequence", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewRemoveFieldOperation(metaDescription.FindField("number"))
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s default value has been dropped
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			//check sequence has been dropped
			Expect(metaDdlFromDB.Seqs).To(HaveLen(1))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})
	})

	Describe("Inner FK field case", func() {
		//setup MetaObj
		BeforeEach(func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			//MetaDescription B
			bMetaDescription := &meta.Meta{
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
					{
						Name: "number",
						Type: meta.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					},
				},
			}
			operation := object.NewCreateObjectOperation(bMetaDescription)

			bMetaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			err = operation.SyncDbDescription(bMetaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//MetaDescription A
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
					{
						Name:     "b",
						Type:     meta.FieldTypeObject,
						LinkMeta: bMetaDescription,
						LinkType: meta.LinkTypeInner,
					},
				},
			}
			//create MetaDescription
			operation = object.NewCreateObjectOperation(metaDescription)
			//sync MetaDescription
			metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("Drops IFK if field is being dropped", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewRemoveFieldOperation(metaDescription.FindField("b"))
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that IFK has been dropped
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
			globalTransactionManager.CommitTransaction(globalTransaction)

			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.IFKs).To(HaveLen(0))

		})
	})
})
