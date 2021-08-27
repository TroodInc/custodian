package object

import (
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'CreateObject' Migration Operation", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache(), db)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)

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
		Expect(err).To(BeNil())

		operation := NewCreateObjectOperation(metaDescription)
		metaDescription, err = operation.SyncMetaDescription(nil, metaDescriptionSyncer)
		Expect(err).To(BeNil())

		//sync MetaDescription with DB
		err = operation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
		Expect(err).To(BeNil())
		tx := globalTransaction.Transaction()

		//ensure table has been created
		metaDdlFromDB, err := object.MetaDDLFromDB(tx, metaDescription.Name)
		Expect(err).To(BeNil())
		Expect(metaDdlFromDB).NotTo(BeNil())

		globalTransaction.Rollback()
	})
})
