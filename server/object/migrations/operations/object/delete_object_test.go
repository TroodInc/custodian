package object

import (
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"
	"database/sql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'DeleteObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
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
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		if err != nil {
			dbTransactionManager.RollbackTransaction(globalTransaction)
			Expect(err).To(BeNil())
		}
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
		//sync its MetaDescription
		err = syncer.CreateObj(dbTransactionManager, metaDescription, metaDescriptionSyncer)
		if err != nil {
			dbTransactionManager.RollbackTransaction(globalTransaction)
			Expect(err).To(BeNil())
		}

		dbTransactionManager.CommitTransaction(globalTransaction)
	})

	//setup teardown
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("removes MetaDescription`s file", func() {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		if err != nil {
			dbTransactionManager.RollbackTransaction(globalTransaction)
			Expect(err).To(BeNil())
		}

		//remove MetaDescription from DB
		metaName := metaDescription.Name
		err = new(DeleteObjectOperation).SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
		if err != nil {
			dbTransactionManager.RollbackTransaction(globalTransaction)
			Expect(err).To(BeNil())
		}

		tx := globalTransaction.Transaction().(*sql.Tx)

		//ensure table has been removed
		metaDdlFromDB, err := object.MetaDDLFromDB(tx, metaName)
		Expect(err).NotTo(BeNil())
		Expect(metaDdlFromDB).To(BeNil())
		dbTransactionManager.CommitTransaction(globalTransaction)

		//	ensure meta file does not exist
		metaDescription, _, err := metaDescriptionSyncer.Get(metaName)
		Expect(metaDescription).To(BeNil())
	})
})
