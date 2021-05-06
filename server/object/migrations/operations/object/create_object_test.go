package object

import (
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"
	"database/sql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'CreateObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := object.NewStore(metaDescriptionSyncer, syncer, dbTransactionManager)

	var metaDescription *description.MetaDescription

	//setup transaction
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	//setup MetaDescription
	BeforeEach(func() {
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
			},
		}
	})

	//setup teardown
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("creates corresponding table in the database", func() {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		if err != nil {
			dbTransactionManager.RollbackTransaction(globalTransaction)
			Expect(err).To(BeNil())
		}

		operation := NewCreateObjectOperation(metaDescription)
		metaDescription, err = operation.SyncMetaDescription(nil, metaDescriptionSyncer)
		if err != nil {
			dbTransactionManager.RollbackTransaction(globalTransaction)
			Expect(err).To(BeNil())
		}

		//sync MetaDescription with DB
		err = operation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
		if err != nil {
			dbTransactionManager.RollbackTransaction(globalTransaction)
			Expect(err).To(BeNil())
		}
		tx := globalTransaction.Transaction().(*sql.Tx)

		//ensure table has been created
		metaDdlFromDB, err := object.MetaDDLFromDB(tx, metaDescription.Name)
		if err != nil {
			dbTransactionManager.RollbackTransaction(globalTransaction)
			Expect(err).To(BeNil())
		}
		Expect(metaDdlFromDB).NotTo(BeNil())

		dbTransactionManager.RollbackTransaction(globalTransaction)
	})
})
