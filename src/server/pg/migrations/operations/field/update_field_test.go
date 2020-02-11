package field

import (
	"database/sql"
	"github.com/getlantern/deepcopy"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"server/object/meta"
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
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	var metaDescription *meta.Meta
	var fieldToUpdate meta.Field

	flushDb := func() {
		//Flush meta/database
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	}

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
			//sync MetaDescription
			metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//

			// clone a field
			field := metaDescription.FindField("number")
			err = deepcopy.Copy(&fieldToUpdate, *field)
			Expect(err).To(BeNil())
			Expect(fieldToUpdate).NotTo(BeNil())

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("changes column type", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Type = meta.FieldTypeString

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(meta.FieldTypeString))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("changes nullability flag", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Optional = false

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Optional).To(BeFalse())

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("changes name", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Name = "updated-name"

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Name).To(Equal("updated-name"))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("drops default value", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Def = ""
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s default value has been dropped
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Defval).To(Equal(""))
			//check sequence has been dropped
			Expect(metaDdlFromDB.Seqs).To(HaveLen(1))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("does all things described above at once", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//
			field := meta.Field{Name: "new-number", Type: meta.FieldTypeString, Optional: false, Def: nil}

			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &field)

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
			//Optional has changed
			Expect(metaDdlFromDB.Columns[1].Optional).To(BeFalse())
			//Type has changed
			Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(meta.FieldTypeString))
			//Name has changed
			Expect(metaDdlFromDB.Columns[1].Name).To(Equal("new-number"))
			//Default has been dropped
			Expect(metaDdlFromDB.Columns[1].Defval).To(Equal(""))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("creates default value and sequence", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//prepare field, set its default value to nil
			fieldToUpdate.Def = ""

			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			metaDescription, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//

			//modify field
			fieldToUpdate.Def = map[string]interface{}{
				"func": "nextval",
			}

			//apply operation
			fieldOperation = NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s default value has been dropped
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
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
			operation := object.NewCreateObjectOperation(bMetaDescription, )

			bMetaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
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

			operation = object.NewCreateObjectOperation(metaDescription)

			//sync MetaDescription
			metaDescription, err = operation.SyncMetaDescription(nil, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(nil, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//

			// clone a field
			field := metaDescription.FindField("b")
			err = deepcopy.Copy(&fieldToUpdate, *field)
			Expect(err).To(BeNil())
			Expect(fieldToUpdate).NotTo(BeNil())

			globalTransactionManager.CommitTransaction(globalTransaction)
		})

		It("changes IFK name if field is renamed", func() {
			globalTransaction, err := globalTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Name = "b_link"

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("b"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, globalTransaction.MetaDescriptionTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := pg.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Name).To(Equal("b_link"))
			Expect(metaDdlFromDB.IFKs).To(HaveLen(1))
			Expect(metaDdlFromDB.IFKs[0].FromColumn).To(Equal("b_link"))

			globalTransactionManager.CommitTransaction(globalTransaction)
		})
	})
})
