package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/meta"
	"server/object/description"
	"server/pg"
	"server/data"
	"server/auth"
	"utils"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
)

var _ = Describe("Create test", func() {
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
				Optional: false,
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

	It("can create a record containing null value of foreign key field", func() {

		Context("having Reason object", func() {
			reasonMetaDescription := description.MetaDescription{
				Name: "reason",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name:     "id",
						Type:     description.FieldTypeString,
						Optional: true,
					},
				},
			}
			reasonMetaObj, err := metaStore.NewMeta(&reasonMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, reasonMetaObj)
			Expect(err).To(BeNil())

			Context("and Lead object referencing Reason object", func() {
				leadMetaDescription := description.MetaDescription{
					Name: "lead",
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
							Name: "name",
							Type: description.FieldTypeString,
						},
						{
							Name:     "decline_reason",
							Type:     description.FieldTypeObject,
							Optional: true,
							LinkType: description.LinkTypeInner,
							LinkMeta: "reason",
						},
					},
				}
				leadMetaObj, err := metaStore.NewMeta(&leadMetaDescription)
				Expect(err).To(BeNil())
				metaStore.Create(globalTransaction, leadMetaObj)
				Context("Lead record with empty reason is created", func() {
					leadData := map[string]interface{}{
						"name": "newLead",
					}
					user := auth.User{}
					record, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, leadMetaDescription.Name, leadData, user)
					Expect(err).To(BeNil())
					Expect(record).To(HaveKey("decline_reason"))
				})
			})
		})
	})

	It("can create record without specifying any value", func() {
		Context("having an object with optional fields", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
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
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(globalTransaction, metaObj)

			Context("DataManager creates record without any values", func() {
				record, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaDescription.Name, map[string]interface{}{}, auth.User{})
				Expect(err).To(BeNil())
				Expect(record["id"]).To(BeEquivalentTo(1))
			})
		})
	})

	It("can create record with null value for optional field", func() {
		Context("having an object", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
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
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(globalTransaction, metaObj)

			recordOne, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaDescription.Name, map[string]interface{}{"name": nil}, auth.User{})
			Expect(err).To(BeNil())
			Expect(recordOne["name"]).To(BeNil())
		})
	})

	It("Can create records containing reserved words", func() {
		Context("having an object named by reserved word and containing field named by reserved word", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
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
						Name: "order",
						Type: description.FieldTypeString,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, metaObj)
			Expect(err).To(BeNil())
			Context("and record has values containing reserved word", func() {
				record, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaDescription.Name, map[string]interface{}{"order": "order"}, auth.User{})
				Expect(err).To(BeNil())
				Expect(record["id"]).To(Equal(float64(1)))

			})

		})

	})

	It("Can insert numeric value into string field", func() {
		Context("having an object with string field", func() {
			metaDescription := description.MetaDescription{
				Name: "order",
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
			metaObj, _ := metaStore.NewMeta(&metaDescription)
			metaStore.Create(globalTransaction, metaObj)

			Context("record can contain numeric value for string field", func() {
				record, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, metaDescription.Name, map[string]interface{}{"name": 202}, auth.User{})
				Expect(err).To(BeNil())
				Expect(record["name"]).To(Equal("202"))
			})
		})
	})

	It("can create record with nested inner record at once", func() {
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		bData := map[string]interface{}{
			"a": map[string]interface{}{"name": "A record"},
		}
		user := auth.User{}
		record, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, bData, user)
		Expect(err).To(BeNil())
		Expect(record).To(HaveKey("a"))
		aData := record["a"].(map[string]interface{})
		Expect(aData).To(HaveKey("id"))
		Expect(aData["id"]).To(BeAssignableToTypeOf(1.0))
	})

	It("can create record with nested outer records at once", func() {
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		aData := map[string]interface{}{
			"name":   "A record",
			"b__set": []interface{}{map[string]interface{}{"name": "B record"}},
		}
		user := auth.User{}
		record, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaDescription.Name, aData, user)
		Expect(err).To(BeNil())
		Expect(record).To(HaveKey("b__set"))
		bSetData := record["b__set"].([]interface{})
		Expect(bSetData).To(HaveLen(1))
		bRecordData := bSetData[0].(map[string]interface{})
		Expect(bRecordData["id"]).To(BeAssignableToTypeOf(1.0))
	})

	It("can create record with nested outer records of mixed types(both new and existing) at once", func() {
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, bMetaObj)
		Expect(err).To(BeNil())

		user := auth.User{}
		anotherARecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaDescription.Name, map[string]interface{}{}, user)

		bRecord, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, bMetaDescription.Name, map[string]interface{}{"name": "Existing B record", "a": anotherARecord["id"]}, user)
		Expect(err).To(BeNil())

		aData := map[string]interface{}{
			"name":   "A record",
			"b__set": []interface{}{map[string]interface{}{"name": "B record"}, bRecord["id"]},
		}

		record, err := dataProcessor.CreateRecord(globalTransaction.DbTransaction, aMetaDescription.Name, aData, user)
		Expect(err).To(BeNil())
		Expect(record).To(HaveKey("b__set"))
		bSetData := record["b__set"].([]interface{})
		Expect(bSetData).To(HaveLen(2))

		newBRecordData := bSetData[0].(map[string]interface{})
		Expect(newBRecordData["id"]).To(BeAssignableToTypeOf(1.0))
		Expect(newBRecordData["name"].(string)).To(Equal("B record"))

		existingBRecordData := bSetData[1].(map[string]interface{})
		Expect(existingBRecordData["id"]).To(BeAssignableToTypeOf(1.0))
		Expect(existingBRecordData["name"].(string)).To(Equal("Existing B record"))
	})
})
