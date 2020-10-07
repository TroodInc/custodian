package field

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"custodian/server/pg"
	"custodian/utils"
	"custodian/server/object/meta"
	"custodian/server/transactions/file_transaction"
	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions"
	"custodian/server/object/description"
	"custodian/server/pg/migrations/operations/object"
	"database/sql"
	"custodian/server/pg_meta"
)

var _ = Describe("'AddField' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg_meta.NewPgMetaDescriptionSyncer(dbTransactionManager)
	//transaction managers
	fileMetaTransactionManager := file_transaction.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer.Remove, metaDescriptionSyncer.Create)
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

	Describe("Enum field case", func() {

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
						Name:     "enumField",
						Type:     description.FieldTypeEnum,
						Optional: true,
						Enum:     description.EnumChoices{"string", "ping", "wing"},
					},
				},
			}
			//create MetaDescription
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
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
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			Expect(err).To(BeNil())

			//apply operation
			removeFieldOperation := NewRemoveFieldOperation(metaDescription.FindField("enumField"))
			err = removeFieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = removeFieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns).To(HaveLen(1))
			globalTransactionManager.CommitTransaction(globalTransaction)
		})
	})

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
			//create MetaDescription
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
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
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
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
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
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
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			//MetaDescription B
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

			bMetaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			err = operation.SyncDbDescription(bMetaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//MetaDescription A
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
						LinkMeta: bMetaDescription.Name,
						LinkType: description.LinkTypeInner,
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
			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
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
