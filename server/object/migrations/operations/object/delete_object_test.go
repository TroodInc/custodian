package object

import (
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'DeleteObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

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
		//sync its MetaDescription
		err = metaStore.CreateObj(metaDescription, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		dbTransactionManager.CommitTransaction(globalTransaction)
	})

	//setup teardown
	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("removes MetaDescription`s file", func() {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())

		//remove MetaDescription from DB
		metaName := metaDescription.Name
		err = new(DeleteObjectOperation).SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		tx := globalTransaction.Transaction()

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
