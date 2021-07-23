package field

import (
	object2 "custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/migrations/operations/object"

	"custodian/utils"
	"fmt"

	"github.com/getlantern/deepcopy"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("'UpdateField' Migration Operation", func() {
	appConfig := utils.GetConfig()
	db, _ := object2.NewDbConnection(appConfig.DbConnectionUrl)

	dbTransactionManager := object2.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object2.NewPgMetaDescriptionSyncer(dbTransactionManager, object2.NewCache())
	metaStore := object2.NewStore(metaDescriptionSyncer, dbTransactionManager)

	var metaDescription *description.MetaDescription
	var fieldToUpdate description.Field

	flushDb := func() {
		//Flush meta/database
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	}
	AfterEach(flushDb)

	testObjAName := utils.RandomString(8)
	testObjBName := utils.RandomString(8)

	Describe("Simple field case", func() {

		//setup MetaObj
		JustBeforeEach(func() {
			//"Direct" case
			metaDescription = &description.MetaDescription{
				Name: testObjAName,
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
			//sync MetaDescription
			metaDescription, err = operation.SyncMetaDescription(nil, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(nil, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//

			// clone a field
			field := metaDescription.FindField("number")
			err = deepcopy.Copy(&fieldToUpdate, *field)
			Expect(err).To(BeNil())
			Expect(fieldToUpdate).NotTo(BeNil())

			globalTransaction.Commit()
		})

		It("changes column type", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Type = description.FieldTypeString

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.Transaction()
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(description.FieldTypeString))

			globalTransaction.Commit()
		})

		It("changes nullability flag", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Optional = false

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.Transaction()
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Optional).To(BeFalse())

			globalTransaction.Commit()
		})

		It("changes name", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Name = "updated-name"

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.Transaction()
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Name).To(Equal("updated-name"))

			globalTransaction.Commit()
		})

		It("drops default value", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Def = ""
			Expect(err).To(BeNil())

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s default value has been dropped
			tx := globalTransaction.Transaction()
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Defval).To(Equal(""))
			//check sequence has been dropped
			Expect(metaDdlFromDB.Seqs).To(HaveLen(1))

			globalTransaction.Commit()
		})

		It("does all things described above at once", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//
			field := description.Field{Name: "new-number", Type: description.FieldTypeString, Optional: false, Def: nil}

			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &field)

			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			tx := globalTransaction.Transaction()
			//
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB).NotTo(BeNil())
			Expect(metaDdlFromDB.Columns).To(HaveLen(2))
			//Optional has changed
			Expect(metaDdlFromDB.Columns[1].Optional).To(BeFalse())
			//Type has changed
			Expect(metaDdlFromDB.Columns[1].Typ).To(Equal(description.FieldTypeString))
			//Name has changed
			Expect(metaDdlFromDB.Columns[1].Name).To(Equal("new-number"))
			//Default has been dropped
			Expect(metaDdlFromDB.Columns[1].Defval).To(Equal(""))

			globalTransaction.Commit()
		})

		It("creates default value and sequence", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//prepare field, set its default value to nil
			fieldToUpdate.Def = ""

			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			metaDescription, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//

			//modify field
			fieldToUpdate.Def = map[string]interface{}{
				"func": "nextval",
			}

			//apply operation
			fieldOperation = NewUpdateFieldOperation(metaDescription.FindField("number"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s default value has been dropped
			tx := globalTransaction.Transaction()
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Defval).To(Equal(fmt.Sprintf("nextval('o_%s_number_seq'::regclass)", testObjAName)))
			//check sequence has been dropped
			Expect(metaDdlFromDB.Seqs).To(HaveLen(2))

			globalTransaction.Commit()
		})
	})

	Describe("Inner FK field case", func() {
		//setup MetaObj
		BeforeEach(func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			//MetaDescription B
			bMetaDescription := &description.MetaDescription{
				Name: testObjBName,
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
			err = operation.SyncDbDescription(nil, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//MetaDescription A
			metaDescription = &description.MetaDescription{
				Name: testObjAName,
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

			operation = object.NewCreateObjectOperation(metaDescription)

			//sync MetaDescription
			metaDescription, err = operation.SyncMetaDescription(nil, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(nil, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//

			// clone a field
			field := metaDescription.FindField("b")
			err = deepcopy.Copy(&fieldToUpdate, *field)
			Expect(err).To(BeNil())
			Expect(fieldToUpdate).NotTo(BeNil())

			globalTransaction.Commit()
		})

		It("changes IFK name if field is renamed", func() {
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())

			//modify field
			fieldToUpdate.Name = "b_link"

			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("b"), &fieldToUpdate)
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.Transaction()
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Name).To(Equal("b_link"))
			Expect(metaDdlFromDB.IFKs).To(HaveLen(1))
			Expect(metaDdlFromDB.IFKs[0].FromColumn).To(Equal("b_link"))

			globalTransaction.Commit()
		})
	})

	Describe("Enum field case", func() {
		//setup MetaObj
		JustBeforeEach(func() {
			//"Direct" case
			metaDescription = &description.MetaDescription{
				Name: testObjBName,
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
						Name:     "enum",
						Type:     description.FieldTypeEnum,
						Enum:     []string{"val1", "val2"},
						Optional: true,
					},
				},
			}
			//create MetaDescription
			operation := object.NewCreateObjectOperation(metaDescription)
			//sync MetaDescription
			metaDescription, err := operation.SyncMetaDescription(nil, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB

			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())
			err = operation.SyncDbDescription(nil, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			err = globalTransaction.Commit()
			Expect(err).To(BeNil())
			//

			// clone a field
			field := metaDescription.FindField("enum")
			err = deepcopy.Copy(&fieldToUpdate, *field)
			Expect(err).To(BeNil())
			Expect(fieldToUpdate).NotTo(BeNil())

		})

		It("set default", func() {
			enumExistingVal := "val1"
			//set default value
			fieldToUpdate.Def = enumExistingVal
			fieldToUpdate.Enum = nil
			//apply operation
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("enum"), &fieldToUpdate)
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			//check that field`s type has changed
			tx := globalTransaction.Transaction()
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			globalTransaction.Commit()
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Defval).To(Equal(fmt.Sprintf("'val1'::o_%s_enum", testObjBName)))

		})
	})

	Describe("Add choice to Enum field", func() {
		//setup MetaObj
		JustBeforeEach(func() {
			//"Direct" case
			metaDescription = &description.MetaDescription{
				Name: testObjBName,
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
						Name:     "enum_field",
						Type:     description.FieldTypeEnum,
						Enum:     []string{"val1", "val2"},
						Optional: true,
					},
				},
			}
			//create MetaDescription
			operation := object.NewCreateObjectOperation(metaDescription)
			//sync MetaDescription
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			Expect(err).To(BeNil())
			metaDescription, err = operation.SyncMetaDescription(nil, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			//sync DB
			err = operation.SyncDbDescription(nil, globalTransaction, metaDescriptionSyncer)
			globalTransaction.Commit()
			Expect(err).To(BeNil())
			//

			// clone a field
			field := metaDescription.FindField("enum_field")
			err = deepcopy.Copy(&fieldToUpdate, *field)
			Expect(err).To(BeNil())
			Expect(fieldToUpdate).NotTo(BeNil())
		})

		It("add value to enum", func() {
			fieldToUpdate.Enum = []string{"val1", "val2", "val3"}

			//apply operation
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("enum_field"), &fieldToUpdate)
			Expect(err).To(BeNil())
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			tx := globalTransaction.Transaction()
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(len(metaDdlFromDB.Columns[1].Enum)).To(Equal(3))
			Expect(metaDdlFromDB.Columns[1].Enum[2]).To(Equal("val3"))
			globalTransaction.Commit()
		})

		It("rename enum column", func() {
			fieldToUpdate.Name = "enum_field_new"

			//apply operation
			globalTransaction, err := dbTransactionManager.BeginTransaction()
			fieldOperation := NewUpdateFieldOperation(metaDescription.FindField("enum_field"), &fieldToUpdate)
			Expect(err).To(BeNil())
			err = fieldOperation.SyncDbDescription(metaDescription, globalTransaction, metaDescriptionSyncer)
			Expect(err).To(BeNil())
			_, err = fieldOperation.SyncMetaDescription(metaDescription, metaDescriptionSyncer)
			Expect(err).To(BeNil())

			tx := globalTransaction.Transaction()
			metaDdlFromDB, err := object2.MetaDDLFromDB(tx, metaDescription.Name)
			Expect(err).To(BeNil())
			Expect(metaDdlFromDB.Columns[1].Name).To(Equal("enum_field_new"))
			globalTransaction.Commit()
		})
	})
})
