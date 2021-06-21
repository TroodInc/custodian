package object

import (
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"
	"database/sql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'RenameObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)

	var metaDescription *description.MetaDescription
	//setup transaction
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	//setup MetaDescription
	BeforeEach(func() {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
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
		Expect(err).To(BeNil())

		//sync its MetaDescription with MetaDescription storage
		err = metaDescriptionSyncer.Create(*metaDescription)
		Expect(err).To(BeNil())
		//sync its MetaDescription with DB
		err = metaStore.CreateObj(dbTransactionManager, metaDescription, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		dbTransactionManager.CommitTransaction(globalTransaction)
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

		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		//sync MetaDescription with DB
		operation := NewRenameObjectOperation(newMetaDescription)
		err = operation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		tx := globalTransaction.Transaction().(*sql.Tx)

		//ensure table has been renamed
		metaDdlFromDB, err := object.MetaDDLFromDB(tx, newMetaDescription.Name)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())

		//ensure table with old name does not exist
		oldMetaDdlFromDB, err := object.MetaDDLFromDB(tx, oldMetaName)
		Expect(err).NotTo(BeNil())
		Expect(oldMetaDdlFromDB).To(BeNil())

		dbTransactionManager.RollbackTransaction(globalTransaction)
	})
})
