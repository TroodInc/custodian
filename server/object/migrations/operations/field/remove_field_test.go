package field

import (
	object2 "custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/migrations/operations/object"

	"custodian/utils"
	"database/sql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'AddField' Migration Operation", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object2.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	dbTransactionManager := object2.NewPgDbTransactionManager(dataManager)

	metaDescriptionSyncer := object2.NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := object2.NewStore(metaDescriptionSyncer, syncer, dbTransactionManager)

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
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			operation := object.NewCreateObjectOperation(metaDescription)

			metaDescription, err = operation.SyncMetaDescription(nil, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//
			dbTransactionManager.CommitTransaction(globalTransaction)

		})

		It("drops column", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//apply operation
			removeFieldOperation := NewRemoveFieldOperation(metaDescription.FindField("enumField"))
			err = removeFieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = removeFieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			tx := globalTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns).To(HaveLen(1))
			dbTransactionManager.CommitTransaction(globalTransaction)
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
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			operation := object.NewCreateObjectOperation(metaDescription)

			metaDescription, err = operation.SyncMetaDescription(nil, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//
			dbTransactionManager.CommitTransaction(globalTransaction)

		})

		It("drops column", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewRemoveFieldOperation(metaDescription.FindField("number"))
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field has been removed
			tx := globalTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns).To(HaveLen(1))

			dbTransactionManager.CommitTransaction(globalTransaction)
		})

		It("drops sequence", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewRemoveFieldOperation(metaDescription.FindField("number"))
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s default value has been dropped
			tx := globalTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			//check sequence has been dropped
			Expect(metaDdlFromDB.Seqs).To(HaveLen(1))

			dbTransactionManager.CommitTransaction(globalTransaction)
		})
	})

	Describe("Inner FK field case", func() {
		//setup MetaObj
		BeforeEach(func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
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

			bMetaDescription, err = operation.SyncMetaDescription(nil, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			err = operation.SyncDbDescription(bMetaDescription, globalTransaction, metaDescriptionSyncer)
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
			metaDescription, err = operation.SyncMetaDescription(nil, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(nil, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//

			dbTransactionManager.CommitTransaction(globalTransaction)
		})

		It("Drops IFK if field is being dropped", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewRemoveFieldOperation(metaDescription.FindField("b"))
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that IFK has been dropped
			tx := globalTransaction.Transaction().(*sql.Tx)
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			dbTransactionManager.CommitTransaction(globalTransaction)

			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.IFKs).To(HaveLen(0))

		})
	})
})
