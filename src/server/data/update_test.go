package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"server/data"
	"server/auth"
	"strconv"
	"utils"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/object/meta"
	"server/object/description"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	var globalTransaction *transactions.GlobalTransaction

	BeforeEach(func() {
		var err error
		globalTransaction, err = globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		metaStore.Flush(globalTransaction)
	})

	AfterEach(func() {
		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("Can update records containing reserved words", func() {
		Context("having an object named by reserved word and containing field named by reserved word", func() {

			metaDescription := description.MetaDescription{
				Name: "order",
				Key:  "order",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "order",
						Type:     description.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name: "select",
						Type: description.FieldTypeString,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, metaObj)
			Expect(err).To(BeNil())

			Context("and record of this object", func() {
				record, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaObj.Name, map[string]interface{}{"select": "some value"}, auth.User{})
				Expect(err).To(BeNil())
				Context("is being updated with values containing reserved word", func() {
					record, err := dataProcessor.UpdateRecord(globalTransaction.DbTransaction, metaObj.Name, strconv.Itoa(int(record["order"].(float64))), map[string]interface{}{"select": "select"}, auth.User{})
					Expect(err).To(BeNil())
					Expect(record["select"]).To(Equal("select"))
				})

			})

		})

	})

	It("Can perform bulk update", func() {
		By("Having Position object")

		positionMetaDescription := description.MetaDescription{
			Name: "position",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name: "name",
					Type: description.FieldTypeString,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&positionMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, metaObj)
		Expect(err).To(BeNil())

		By("and Person object")

		metaDescription := description.MetaDescription{
			Name: "person",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "position",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "position",
				},
				{
					Name: "name",
					Type: description.FieldTypeString,
				},
			},
		}
		metaObj, err = metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, metaObj)
		Expect(err).To(BeNil())

		By("and having one record of Position object")
		positionRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, positionMetaDescription.Name, map[string]interface{}{"name": "manager"}, auth.User{})
		Expect(err).To(BeNil())

		By("and having two records of Person object")

		records := make([]map[string]interface{}, 2)

		records[0], err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaDescription.Name, map[string]interface{}{"name": "Ivan", "position": positionRecord["id"]}, auth.User{})
		Expect(err).To(BeNil())

		records[1], err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaDescription.Name, map[string]interface{}{"name": "Vasily", "position": positionRecord["id"]}, auth.User{})
		Expect(err).To(BeNil())

		updatedRecords := make([]map[string]interface{}, 0)

		Context("person records are updated with new name value and new position`s name value as nested object", func() {
			counter := 0
			next := func() (map[string]interface{}, error) {
				if counter < len(records) {
					records[counter]["name"] = "Victor"
					records[counter]["position"] = map[string]interface{}{"id": positionRecord["id"], "name": "sales manager"}
					defer func() { counter += 1 }()
					return records[counter], nil
				}
				return nil, nil
			}

			sink := func(record map[string]interface{}) error {
				updatedRecords = append(updatedRecords, record)
				return nil
			}

			err := dataProcessor.BulkUpdateRecords(globalTransaction.DbTransaction, metaDescription.Name, next, sink, auth.User{})
			Expect(err).To(BeNil())

			Expect(updatedRecords[0]["name"]).To(Equal("Victor"))

			positionRecord, _ = updatedRecords[0]["position"].(map[string]interface{})
			Expect(positionRecord["name"]).To(Equal("sales manager"))

		})

	})

	It("Can perform update", func() {
		By("Having Position object")

		positionMetaDescription := description.MetaDescription{
			Name: "position",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name: "name",
					Type: description.FieldTypeString,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&positionMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, metaObj)
		Expect(err).To(BeNil())

		By("and having one record of Position object")
		recordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, positionMetaDescription.Name, map[string]interface{}{"name": "manager"}, auth.User{})
		Expect(err).To(BeNil())

		keyValue, _ := recordData["id"].(float64)
		Context("person records are updated with new name value and new position`s name value as nested object", func() {
			recordData["name"] = "sales manager"
			recordData, err := dataProcessor.UpdateRecord(globalTransaction.DbTransaction, positionMetaDescription.Name, strconv.Itoa(int(keyValue)), recordData, auth.User{})
			Expect(err).To(BeNil())

			Expect(recordData["name"]).To(Equal("sales manager"))

		})

	})

	It("Can update record with null value", func() {
		By("Having A object")

		positionMetaDescription := description.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&positionMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, metaObj)
		Expect(err).To(BeNil())

		By("and having one record of A object")
		recordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, positionMetaDescription.Name, map[string]interface{}{"name": ""}, auth.User{}, )
		Expect(err).To(BeNil())

		keyValue, _ := recordData["id"].(float64)
		Context("person records are updated with new name value and new position`s name value as nested object", func() {
			recordData["name"] = nil
			recordData, err := dataProcessor.UpdateRecord(globalTransaction.DbTransaction, positionMetaDescription.Name, strconv.Itoa(int(keyValue)), recordData, auth.User{})
			Expect(err).To(BeNil())

			Expect(recordData["name"]).To(BeNil())
		})

	})

	aMetaDescription := description.MetaDescription{
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
				Optional: true,
			},
			{
				Name:     "name",
				Type:     description.FieldTypeString,
				Optional: true,
			},
		},
	}

	bMetaDescription := description.MetaDescription{
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
				Optional: true,
			},
			{
				Name:     "a",
				Type:     description.FieldTypeObject,
				Optional: true,
				LinkType: description.LinkTypeInner,
				LinkMeta: "a",
				OnDelete: description.OnDeleteCascade.ToVerbose(),
			},
			{
				Name:     "name",
				Type:     description.FieldTypeString,
				Optional: true,
			},
		},
	}

	It("Can perform update of record with nested inner record at once", func() {
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		aRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaDescription.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, map[string]interface{}{"name": "B record", "a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())

		bPkValue, _ := bMetaObj.Key.ValueAsString(bRecordData[bMetaDescription.Key])
		bUpdatedData := map[string]interface{}{
			"id":          bPkValue,
			"name":        "Updated B name",
			aMetaObj.Name: map[string]interface{}{"id": aRecordData["id"], "name": "Updated A name"},
		}
		recordData, err := dataProcessor.UpdateRecord(globalTransaction.DbTransaction, bMetaObj.Name, bPkValue, bUpdatedData, auth.User{})
		Expect(err).To(BeNil())
		Expect(recordData).To(HaveKey("a"))
	})

	It("Can perform update of record with nested inner record at once", func() {
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		aRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaDescription.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, map[string]interface{}{"name": "B record", "a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())

		bPkValue, _ := bMetaObj.Key.ValueAsString(bRecordData[bMetaDescription.Key])
		bUpdatedData := map[string]interface{}{
			"id":          bPkValue,
			"name":        "Updated B name",
			aMetaObj.Name: map[string]interface{}{"id": aRecordData["id"], "name": "Updated A name"},
		}
		recordData, err := dataProcessor.UpdateRecord(globalTransaction.DbTransaction, bMetaObj.Name, bPkValue, bUpdatedData, auth.User{})
		Expect(err).To(BeNil())
		Expect(recordData).To(HaveKey("a"))
	})

	It("Can perform update of record with nested outer records of mixed types: new record, existing record`s PK and existing record`s new data at once", func() {
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		aRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaDescription.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, map[string]interface{}{"name": "B record", "a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())

		anotherBRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, map[string]interface{}{"name": "Another B record", "a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aPkValue, _ := aMetaObj.Key.ValueAsString(aRecordData[aMetaDescription.Key])

		aUpdateData := map[string]interface{}{
			"id":   aPkValue,
			"name": "Updated A name",
			"b_set": []interface{}{
				map[string]interface{}{"id": bRecordData["id"], "name": "Updated B name"}, //existing record with new data
				anotherBRecordData["id"],                                                  //existing record`s PK
				map[string]interface{}{"name": "New B Record"},                            //new record`s data
			},
		}

		recordData, err := dataProcessor.UpdateRecord(globalTransaction.DbTransaction, aMetaObj.Name, aPkValue, aUpdateData, auth.User{})
		Expect(err).To(BeNil())
		Expect(recordData).To(HaveKey("b_set"))
		bSetData := recordData["b_set"].([]interface{})
		Expect(bSetData).To(HaveLen(3))
		Expect(bSetData[0].(map[string]interface{})["name"]).To(Equal("Updated B name"))
		Expect(bSetData[1].(map[string]interface{})["name"]).To(Equal("Another B record"))
		Expect(bSetData[2].(map[string]interface{})["name"]).To(Equal("New B Record"))
	})

	It("Processes delete logic for outer records which are not presented in update data. `Cascade` strategy case ", func() {
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		aRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaDescription.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, map[string]interface{}{"name": "B record", "a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())

		anotherBRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, map[string]interface{}{"name": "Another B record", "a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aPkValue, _ := aMetaObj.Key.ValueAsString(aRecordData[aMetaDescription.Key])

		//anotherBRecord`s id is not set
		aUpdateData := map[string]interface{}{
			"id":   aPkValue,
			"name": "Updated A name",
			"b_set": []interface{}{
				bRecordData["id"],                              //existing record with new data
				map[string]interface{}{"name": "New B Record"}, //new record`s data
			},
		}

		recordData, err := dataProcessor.UpdateRecord(globalTransaction.DbTransaction, aMetaObj.Name, aPkValue, aUpdateData, auth.User{})
		Expect(err).To(BeNil())

		//check returned data
		Expect(recordData).To(HaveKey("b_set"))
		bSetData := recordData["b_set"].([]interface{})
		Expect(bSetData).To(HaveLen(2))
		Expect(bSetData[0].(map[string]interface{})["name"]).To(Equal("B record"))
		Expect(bSetData[1].(map[string]interface{})["name"]).To(Equal("New B Record"))
		//	check queried data
		record, err := dataProcessor.Get(globalTransaction.DbTransaction, aMetaObj.Name, aPkValue, 2)
		Expect(err).To(BeNil())
		Expect(record.Data).To(HaveKey("b_set"))
		bSetData = record.Data["b_set"].([]interface{})
		Expect(bSetData).To(HaveLen(2))
		Expect(bSetData[0].(map[string]interface{})["name"]).To(Equal("B record"))
		Expect(bSetData[1].(map[string]interface{})["name"]).To(Equal("New B Record"))
		//	check B record is deleted
		removedBRecordPk, _ := bMetaObj.Key.ValueAsString(anotherBRecord["id"])
		record, err = dataProcessor.Get(globalTransaction.DbTransaction, bMetaObj.Name, removedBRecordPk, 1)
		Expect(err).To(BeNil())
		Expect(record).To(BeNil())
	})

	It("Processes delete logic for outer records which are not presented in update data. `SetNull` strategy case ", func() {
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaDescription := bMetaDescription
		bMetaDescription.Fields[1].OnDelete = description.OnDeleteSetNull.ToVerbose()
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		aRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaDescription.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, map[string]interface{}{"name": "B record", "a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())

		anotherBRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, map[string]interface{}{"name": "Another B record", "a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aPkValue, _ := aMetaObj.Key.ValueAsString(aRecordData[aMetaDescription.Key])

		//anotherBRecord`s id is not set
		aUpdateData := map[string]interface{}{
			"id":   aPkValue,
			"name": "Updated A name",
			"b_set": []interface{}{
				bRecordData["id"],                              //existing record with new data
				map[string]interface{}{"name": "New B Record"}, //new record`s data
			},
		}

		recordData, err := dataProcessor.UpdateRecord(globalTransaction.DbTransaction, aMetaObj.Name, aPkValue, aUpdateData, auth.User{})
		Expect(err).To(BeNil())

		//check returned data
		Expect(recordData).To(HaveKey("b_set"))
		bSetData := recordData["b_set"].([]interface{})
		Expect(bSetData).To(HaveLen(2))
		Expect(bSetData[0].(map[string]interface{})["name"]).To(Equal("B record"))
		Expect(bSetData[1].(map[string]interface{})["name"]).To(Equal("New B Record"))
		//	check queried data
		record, err := dataProcessor.Get(globalTransaction.DbTransaction, aMetaObj.Name, aPkValue, 2)
		Expect(err).To(BeNil())
		Expect(record.Data).To(HaveKey("b_set"))
		bSetData = record.Data["b_set"].([]interface{})
		Expect(bSetData).To(HaveLen(2))
		Expect(bSetData[0].(map[string]interface{})["name"]).To(Equal("B record"))
		Expect(bSetData[1].(map[string]interface{})["name"]).To(Equal("New B Record"))
		//	check B record is not deleted
		removedBRecordPk, _ := bMetaObj.Key.ValueAsString(anotherBRecord["id"])
		record, err = dataProcessor.Get(globalTransaction.DbTransaction, bMetaObj.Name, removedBRecordPk, 1)
		Expect(err).To(BeNil())
		Expect(record).NotTo(BeNil())
	})

	It("Processes delete logic for outer records which are not presented in update data. `Restrict` strategy case ", func() {
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaDescription := bMetaDescription
		bMetaDescription.Fields[1].OnDelete = description.OnDeleteRestrict.ToVerbose()
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		aRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaDescription.Name, map[string]interface{}{"name": "A record"}, auth.User{})
		Expect(err).To(BeNil())

		bRecordData, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, map[string]interface{}{"name": "B record", "a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, map[string]interface{}{"name": "Another B record", "a": aRecordData["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aPkValue, _ := aMetaObj.Key.ValueAsString(aRecordData[aMetaDescription.Key])

		//anotherBRecord`s id is not set
		aUpdateData := map[string]interface{}{
			"id":   aPkValue,
			"name": "Updated A name",
			"b_set": []interface{}{
				bRecordData["id"],                              //existing record with new data
				map[string]interface{}{"name": "New B Record"}, //new record`s data
			},
		}

		_, err = dataProcessor.UpdateRecord(globalTransaction.DbTransaction, aMetaObj.Name, aPkValue, aUpdateData, auth.User{})
		Expect(err).NotTo(BeNil())
	})
})
