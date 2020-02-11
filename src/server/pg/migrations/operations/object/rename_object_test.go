package object

import (
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object"

	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("'RenameObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := object.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	var metaDescription *object.Meta
	//setup transaction
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	//setup MetaDescription
	BeforeEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		metaDescription = &object.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		Expect(err).To(BeNil())

		//sync its MetaDescription with MetaDescription storage
		err = metaDescriptionSyncer.Create(globalTransaction.MetaDescriptionTransaction, metaDescription.Name, metaDescription.ForExport())
		Expect(err).To(BeNil())
		//sync its MetaDescription with DB
		err = syncer.CreateObj(globalTransaction.DbTransaction, metaDescription, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	//setup teardown
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("renames corresponding table in the database", func() {
		oldMetaName := metaDescription.Name

		newMetaDescription := metaDescription.Clone()
		newMetaDescription.Name = "b"

		globalTransaction, err := globalTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		//sync MetaDescription with DB
		operation := NewRenameObjectOperation(newMetaDescription)
		err = operation.SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)

		//ensure table has been renamed
		metaDdlFromDB, err := pg.MetaDDLFromDB(tx, newMetaDescription.Name)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())

		//ensure table with old name does not exist
		oldMetaDdlFromDB, err := pg.MetaDDLFromDB(tx, oldMetaName)
		Expect(err).NotTo(BeNil())
		Expect(oldMetaDdlFromDB).To(BeNil())

		globalTransactionManager.RollbackTransaction(globalTransaction)
	})
})
