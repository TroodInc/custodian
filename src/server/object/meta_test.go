package object

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"utils"
	"server/object/meta"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/object/description"
	"database/sql"
)

var _ = Describe("The PG MetaStore", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	BeforeEach(func() {
		var err error

		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		metaStore.Flush(globalTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	AfterEach(func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		err = metaStore.Flush(globalTransaction)
		Expect(err).To(BeNil())

		globalTransactionManager.CommitTransaction(globalTransaction)
	})

	It("can flush all objects", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())

		Context("once object is created", func() {
			metaDescription := description.MetaDescription{
				Name: "person",
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
					}, {
						Name:     "name",
						Type:     description.FieldTypeString,
						Optional: false,
					}, {
						Name:     "gender",
						Type:     description.FieldTypeString,
						Optional: true,
					},
				},
			}
			meta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, meta)
			Expect(err).To(BeNil())

			globalTransactionManager.CommitTransaction(globalTransaction)

			Context("and 'flush' method is called", func() {
				globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
				Expect(err).To(BeNil())
				metaStore.Flush(globalTransaction)
				globalTransactionManager.CommitTransaction(globalTransaction)

				metaList, _, _ := metaStore.List()
				Expect(*metaList).To(HaveLen(0))
			})
		})
	})

	It("can remove object without leaving orphan outer links", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		defer func() { globalTransactionManager.CommitTransaction(globalTransaction) }()

		Context("having two objects with mutual links", func() {
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
				},
			}
			aMeta, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, aMeta)
			Expect(err).To(BeNil())

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
						Name:     "a_fk",
						Type:     description.FieldTypeObject,
						Optional: true,
						LinkType: description.LinkTypeInner,
						LinkMeta: "a",
					},
				},
			}
			bMeta, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, bMeta)
			Expect(err).To(BeNil())

			aMetaDescription = description.MetaDescription{
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
						Name:           "b_set",
						Type:           description.FieldTypeObject,
						Optional:       true,
						LinkType:       description.LinkTypeOuter,
						LinkMeta:       "b",
						OuterLinkField: "a_fk",
					},
				},
			}
			aMeta, err = metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Update(globalTransaction, aMeta.Name, aMeta, true)

			Context("and 'remove' method is called for B meta", func() {
				metaStore.Remove(globalTransaction, bMeta.Name, true)
				Context("meta A should not contain outer link field which references B meta", func() {
					aMeta, _, _ = metaStore.Get(globalTransaction, aMeta.Name)
					Expect(aMeta.Fields).To(HaveLen(1))
					Expect(aMeta.Fields[0].Name).To(Equal("id"))
				})

			})
		})
	})

	It("can remove object without leaving orphan inner links", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		defer func() { globalTransactionManager.CommitTransaction(globalTransaction) }()

		Context("having two objects with mutual links", func() {
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
				},
			}
			aMeta, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(globalTransaction, aMeta)

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
						Name:     "a_fk",
						Type:     description.FieldTypeObject,
						Optional: true,
						LinkType: description.LinkTypeInner,
						LinkMeta: "a",
					},
				},
			}
			bMeta, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(globalTransaction, bMeta)
			Expect(err).To(BeNil())

			Context("and 'remove' method is called for meta A", func() {
				metaStore.Remove(globalTransaction, aMeta.Name, true)

				Context("meta B should not contain inner link field which references A meta", func() {
					bMeta, _, _ = metaStore.Get(globalTransaction, bMeta.Name)
					Expect(bMeta.Fields).To(HaveLen(1))
					Expect(bMeta.Fields[0].Name).To(Equal("id"))
				})
			})
		})
	})

	It("can remove object`s inner link field without leaving orphan outer links", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		defer func() { globalTransactionManager.CommitTransaction(globalTransaction) }()

		Context("having objects A and B with mutual links", func() {
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
				},
			}
			aMeta, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(globalTransaction, aMeta)

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
						Name:     "a_fk",
						Type:     description.FieldTypeObject,
						Optional: true,
						LinkType: description.LinkTypeInner,
						LinkMeta: "a",
					},
				},
			}
			bMeta, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(globalTransaction, bMeta)
			Expect(err).To(BeNil())

			aMetaDescription = description.MetaDescription{
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
						Name:           "b_set",
						Type:           description.FieldTypeObject,
						Optional:       true,
						LinkType:       description.LinkTypeOuter,
						LinkMeta:       "b",
						OuterLinkField: "a_fk",
					},
				},
			}
			aMeta, err = metaStore.NewMeta(&aMetaDescription)
			metaStore.Update(globalTransaction, aMeta.Name, aMeta, true)
			Expect(err).To(BeNil())

			Context("and inner link field was removed from object B", func() {
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
					},
				}
				bMeta, err := metaStore.NewMeta(&bMetaDescription)
				Expect(err).To(BeNil())
				metaStore.Update(globalTransaction, bMeta.Name, bMeta, true)

				Context("outer link field should be removed from object A", func() {
					aMeta, _, err = metaStore.Get(globalTransaction, aMeta.Name)
					Expect(err).To(BeNil())
					Expect(aMeta.Fields).To(HaveLen(1))
					Expect(aMeta.Fields[0].Name).To(Equal("id"))
				})
			})
		})
	})

	It("checks object for fields with duplicated names when creating object", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		defer func() { globalTransactionManager.CommitTransaction(globalTransaction) }()

		Context("having an object description with duplicated field names", func() {
			metaDescription := description.MetaDescription{
				Name: "person",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id",
						Type: description.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					}, {
						Name:     "name",
						Type:     description.FieldTypeString,
						Optional: false,
					}, {
						Name:     "name",
						Type:     description.FieldTypeString,
						Optional: true,
					},
				},
			}
			Context("When 'NewMeta' method is called it should return error", func() {
				_, err := metaStore.NewMeta(&metaDescription)
				Expect(err).To(Not(BeNil()))
				Expect(err.Error()).To(Equal("Object contains duplicated field 'name'"))
			})
		})
	})

	It("can change field type of existing object", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		defer func() { globalTransactionManager.CommitTransaction(globalTransaction) }()

		By("having an existing object with string field")
		metaDescription := description.MetaDescription{
			Name: "person",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				}, {
					Name:     "name",
					Type:     description.FieldTypeNumber,
					Optional: false,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(globalTransaction, metaObj)
		Expect(err).To(BeNil())

		Context("when object is updated with modified field`s type", func() {
			metaDescription = description.MetaDescription{
				Name: "person",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id",
						Type: description.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					}, {
						Name:     "name",
						Type:     description.FieldTypeString,
						Optional: false,
					},
				},
			}
			meta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(globalTransaction, meta.Name, meta, true)
			Expect(err).To(BeNil())

			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			actualMeta, err := pg.MetaDDLFromDB(tx, meta.Name)
			Expect(err).To(BeNil())

			Expect(err).To(BeNil())
			Expect(actualMeta.Columns[1].Typ).To(Equal(pg.ColumnTypeText))
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'CASCADE' by default", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		defer func() { globalTransactionManager.CommitTransaction(globalTransaction) }()

		By("having an object A")
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
				},
			},
		}
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
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
					}, {
						Name:     "a",
						LinkMeta: aMetaDescription.Name,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						Optional: false,
					},
				},
			}
			aMeta, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, aMeta)
			Expect(err).To(BeNil())

			bMeta, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, bMeta)
			Expect(err).To(BeNil())

			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			//assert schema
			actualMeta, err := pg.MetaDDLFromDB(tx, bMeta.Name)
			Expect(err).To(BeNil())

			Expect(actualMeta.IFKs).To(HaveLen(1))
			Expect(actualMeta.IFKs[0].OnDelete).To(Equal("CASCADE"))

			//assert meta
			bMeta, _, err = metaStore.Get(globalTransaction, bMeta.Name)
			Expect(bMeta.FindField("a").OnDelete).To(Equal(meta.OnDeleteCascade))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'CASCADE' when manually specified", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		defer func() { globalTransactionManager.CommitTransaction(globalTransaction) }()

		By("having an object A")
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
				},
			},
		}
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
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
					}, {
						Name:     "a",
						LinkMeta: aMetaDescription.Name,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						OnDelete: "cascade",
						Optional: false,
					},
				},
			}
			aMeta, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, aMeta)
			Expect(err).To(BeNil())

			bMeta, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, bMeta)
			Expect(err).To(BeNil())

			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			//assert schema
			actualMeta, err := pg.MetaDDLFromDB(tx, bMeta.Name)
			Expect(err).To(BeNil())

			Expect(actualMeta.IFKs).To(HaveLen(1))
			Expect(actualMeta.IFKs[0].OnDelete).To(Equal("CASCADE"))

			//assert meta
			bMeta, _, err = metaStore.Get(globalTransaction, bMeta.Name)
			Expect(bMeta.FindField("a").OnDelete).To(Equal(meta.OnDeleteCascade))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'SET NULL' when manually specified", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		defer func() { globalTransactionManager.CommitTransaction(globalTransaction) }()

		By("having an object A")
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
				},
			},
		}
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
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
					}, {
						Name:     "a",
						LinkMeta: aMetaDescription.Name,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						OnDelete: "setNull",
						Optional: false,
					},
				},
			}
			aMeta, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, aMeta)
			Expect(err).To(BeNil())

			bMeta, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, bMeta)
			Expect(err).To(BeNil())

			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			//assert schema
			actualMeta, err := pg.MetaDDLFromDB(tx, bMeta.Name)
			Expect(err).To(BeNil())

			Expect(actualMeta.IFKs).To(HaveLen(1))
			Expect(actualMeta.IFKs[0].OnDelete).To(Equal("SET NULL"))

			//assert meta
			bMeta, _, err = metaStore.Get(globalTransaction, bMeta.Name)
			Expect(bMeta.FindField("a").OnDelete).To(Equal(meta.OnDeleteSetNull))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'RESTRICT' when manually specified", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		defer func() { globalTransactionManager.CommitTransaction(globalTransaction) }()

		By("having an object A")
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
				},
			},
		}
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
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
					}, {
						Name:     "a",
						LinkMeta: aMetaDescription.Name,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						OnDelete: "restrict",
						Optional: false,
					},
				},
			}
			aMeta, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, aMeta)
			Expect(err).To(BeNil())

			bMeta, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, bMeta)
			Expect(err).To(BeNil())

			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			//assert schema
			actualMeta, err := pg.MetaDDLFromDB(tx, bMeta.Name)
			Expect(err).To(BeNil())

			Expect(actualMeta.IFKs).To(HaveLen(1))
			Expect(actualMeta.IFKs[0].OnDelete).To(Equal("RESTRICT"))

			//assert meta
			bMeta, _, err = metaStore.Get(globalTransaction, bMeta.Name)
			Expect(bMeta.FindField("a").OnDelete).To(Equal(meta.OnDeleteRestrict))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'RESTRICT' when manually specified", func() {
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		Expect(err).To(BeNil())
		defer func() { globalTransactionManager.CommitTransaction(globalTransaction) }()

		By("having an object A")
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
				},
			},
		}
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
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
					}, {
						Name:     "a",
						LinkMeta: aMetaDescription.Name,
						Type:     description.FieldTypeObject,
						LinkType: description.LinkTypeInner,
						OnDelete: "setDefault",
						Optional: false,
					},
				},
			}
			aMeta, err := metaStore.NewMeta(&aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, aMeta)
			Expect(err).To(BeNil())

			bMeta, err := metaStore.NewMeta(&bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(globalTransaction, bMeta)
			Expect(err).To(BeNil())

			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			//assert schema
			actualMeta, err := pg.MetaDDLFromDB(tx, bMeta.Name)
			Expect(err).To(BeNil())

			Expect(actualMeta.IFKs).To(HaveLen(1))
			Expect(actualMeta.IFKs[0].OnDelete).To(Equal("SET DEFAULT"))

			//assert meta
			bMeta, _, err = metaStore.Get(globalTransaction, bMeta.Name)
			Expect(bMeta.FindField("a").OnDelete).To(Equal(meta.OnDeleteSetDefault))
			Expect(err).To(BeNil())
		})
	})
})
