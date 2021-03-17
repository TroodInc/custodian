package object

import (
	"custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/transactions"
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
	globalTransactionManager := transactions.NewGlobalTransactionManager(dbTransactionManager)
	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(globalTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	var metaDescription *description.MetaDescription

	//setup transaction
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	//setup MetaDescription
	BeforeEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
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
		//sync its MetaDescription
		err = syncer.CreateObj(globalTransactionManager, metaDescription, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	//setup teardown
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("removes MetaDescription`s file", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		//remove MetaDescription from DB
		metaName := metaDescription.Name
		err = new(DeleteObjectOperation).SyncDbDescription(metaDescription, globalTransaction.DbTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)

		//ensure table has been removed
		metaDdlFromDB, err := object.MetaDDLFromDB(tx, metaName)
		Expect(err).NotTo(BeNil())
		Expect(metaDdlFromDB).To(BeNil())
		globalTransactionManager.CommitTransaction(globalTransaction)

		//	ensure meta file does not exist
		metaDescription, _, err := metaDescriptionSyncer.Get(metaName)
		Expect(metaDescription).To(BeNil())
	})
})
