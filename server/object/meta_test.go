package object

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"custodian/server/pg"
	"custodian/utils"
	"custodian/server/object/meta"
	"custodian/server/transactions/file_transaction"
	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions"
	"custodian/server/object/description"
	"database/sql"
)

var _ = Describe("The PG MetaStore", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can flush all objects", func() {
		Context("once object is created", func() {
			meta, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
			Expect(err).To(BeNil())
			err = metaStore.Create(meta)
			Expect(err).To(BeNil())

			Context("and 'flush' method is called", func() {
				err := metaStore.Flush()
				Expect(err).To(BeNil())

				metaList, _, _ := metaStore.List()
				Expect(metaList).To(HaveLen(0))
			})
		})
	})

	It("can remove object without leaving orphan outer links", func() {
		Context("having two objects with mutual links", func() {
			aMetaDescription := GetBaseMetaData(utils.RandomString(8))
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMeta)
			Expect(err).To(BeNil())

			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
				Name:     "a_fk",
				Type:     description.FieldTypeObject,
				Optional: true,
				LinkType: description.LinkTypeInner,
				LinkMeta: aMeta.Name,
			})
			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
				Name:           "b_set",
				Type:           description.FieldTypeObject,
				Optional:       true,
				LinkType:       description.LinkTypeOuter,
				LinkMeta:       bMeta.Name,
				OuterLinkField: "a_fk",
			})
			aMeta, err = metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Update(aMeta.Name, aMeta, true)

			Context("and 'remove' method is called for B meta", func() {
				metaStore.Remove(bMeta.Name, true)
				Context("meta A should not contain outer link field which references B meta", func() {
					aMeta, _, _ = metaStore.Get(aMeta.Name, true)
					Expect(aMeta.Fields).To(HaveLen(1))
					Expect(aMeta.Fields[0].Name).To(Equal("id"))
				})

			})
		})
	})

	It("can remove object without leaving orphan inner links", func() {
		Context("having two objects with mutual links", func() {
			aMetaDescription := GetBaseMetaData(utils.RandomString(8))
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMeta)

			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
				Name:     "a_fk",
				Type:     description.FieldTypeObject,
				Optional: true,
				LinkType: description.LinkTypeInner,
				LinkMeta: aMeta.Name,
			})
			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			Context("and 'remove' method is called for meta A", func() {
				metaStore.Remove(aMeta.Name, true)

				Context("meta B should not contain inner link field which references A meta", func() {
					bMeta, _, _ = metaStore.Get(bMeta.Name, true)
					Expect(bMeta.Fields).To(HaveLen(1))
					Expect(bMeta.Fields[0].Name).To(Equal("id"))
				})
			})
		})
	})

	It("can remove object`s inner link field without leaving orphan outer links", func() {
		Context("having objects A and B with mutual links", func() {
			aMetaDescription := GetBaseMetaData(utils.RandomString(8))
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(aMeta)

			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
				Name:     "a_fk",
				Type:     description.FieldTypeObject,
				Optional: true,
				LinkType: description.LinkTypeInner,
				LinkMeta: aMeta.Name,
			})
			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			aMetaDescription.Fields = append(aMetaDescription.Fields, description.Field{
				Name:           "b_set",
				Type:           description.FieldTypeObject,
				Optional:       true,
				LinkType:       description.LinkTypeOuter,
				LinkMeta:       bMeta.Name,
				OuterLinkField: "a_fk",
			})
			aMeta, err = metaStore.NewMeta(aMetaDescription)
			metaStore.Update(aMeta.Name, aMeta, true)
			Expect(err).To(BeNil())

			Context("and inner link field was removed from object B", func() {
				bMetaDescription := GetBaseMetaData(bMetaDescription.Name)
				bMeta, err := metaStore.NewMeta(bMetaDescription)
				Expect(err).To(BeNil())
				metaStore.Update(bMeta.Name, bMeta, true)

				Context("outer link field should be removed from object A", func() {
					aMeta, _, err = metaStore.Get(aMeta.Name, true)
					Expect(err).To(BeNil())
					Expect(aMeta.Fields).To(HaveLen(1))
					Expect(aMeta.Fields[0].Name).To(Equal("id"))
				})
			})
		})
	})

	It("checks object for fields with duplicated names when creating object", func() {
		Context("having an object description with duplicated field names", func() {
			metaDescription := GetBaseMetaData(utils.RandomString(8))
			metaDescription.Fields = append(metaDescription.Fields, []description.Field{
				{
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: false,
				}, {
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			}...)
			Context("When 'NewMeta' method is called it should return error", func() {
				_, err := metaStore.NewMeta(metaDescription)
				Expect(err).To(Not(BeNil()))
				Expect(err.Error()).To(Equal("Object contains duplicated field 'name'"))
			})
		})
	})

	It("can change field type of existing object", func() {
		By("having an existing object with string field")
		metaDescription := GetBaseMetaData(utils.RandomString(8))
		metaDescription.Fields = append(metaDescription.Fields, description.Field{
			Name:     "name",
			Type:     description.FieldTypeNumber,
			Optional: false,
		})
		metaObj, err := metaStore.NewMeta(metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		Context("when object is updated with modified field`s type", func() {
			metaDescription = GetBaseMetaData(metaDescription.Name)
			metaDescription.Fields = append(metaDescription.Fields, description.Field{
				Name:     "name",
				Type:     description.FieldTypeString,
				Optional: false,
			})
			meta, err := metaStore.NewMeta(metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(meta.Name, meta, true)
			Expect(err).To(BeNil())

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			actualMeta, err := pg.MetaDDLFromDB(tx, meta.Name)
			Expect(err).To(BeNil())
			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())

			Expect(err).To(BeNil())
			Expect(actualMeta.Columns[1].Typ).To(Equal(description.FieldTypeString))
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'CASCADE' by default", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B referencing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
				Name:     "a",
				LinkMeta: aMetaDescription.Name,
				Type:     description.FieldTypeObject,
				LinkType: description.LinkTypeInner,
				Optional: false,
			})
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMeta)
			Expect(err).To(BeNil())

			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			//assert schema
			actualMeta, err := pg.MetaDDLFromDB(tx, bMeta.Name)
			Expect(err).To(BeNil())
			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())

			Expect(actualMeta.IFKs).To(HaveLen(1))
			Expect(actualMeta.IFKs[0].OnDelete).To(Equal("CASCADE"))

			//assert meta
			bMeta, _, err = metaStore.Get(bMeta.Name, true)
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(description.OnDeleteCascade))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'CASCADE' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
				Name:     "a",
				LinkMeta: aMetaDescription.Name,
				Type:     description.FieldTypeObject,
				LinkType: description.LinkTypeInner,
				OnDelete: "cascade",
				Optional: false,
			})
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMeta)
			Expect(err).To(BeNil())

			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)

			Expect(err).To(BeNil())

			//assert schema
			actualMeta, err := pg.MetaDDLFromDB(tx, bMeta.Name)
			Expect(err).To(BeNil())
			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())

			Expect(actualMeta.IFKs).To(HaveLen(1))
			Expect(actualMeta.IFKs[0].OnDelete).To(Equal("CASCADE"))

			//assert meta
			bMeta, _, err = metaStore.Get(bMeta.Name, true)
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(description.OnDeleteCascade))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'SET NULL' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
				Name:     "a",
				LinkMeta: aMetaDescription.Name,
				Type:     description.FieldTypeObject,
				LinkType: description.LinkTypeInner,
				OnDelete: "setNull",
				Optional: false,
			})
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMeta)
			Expect(err).To(BeNil())

			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			//assert schema
			actualMeta, err := pg.MetaDDLFromDB(tx, bMeta.Name)
			Expect(err).To(BeNil())
			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())

			Expect(actualMeta.IFKs).To(HaveLen(1))
			Expect(actualMeta.IFKs[0].OnDelete).To(Equal("SET NULL"))

			//assert meta
			bMeta, _, err = metaStore.Get(bMeta.Name, true)
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(description.OnDeleteSetNull))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'RESTRICT' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
				Name:     "a",
				LinkMeta: aMetaDescription.Name,
				Type:     description.FieldTypeObject,
				LinkType: description.LinkTypeInner,
				OnDelete: "restrict",
				Optional: false,
			})
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMeta)
			Expect(err).To(BeNil())

			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			//assert schema
			actualMeta, err := pg.MetaDDLFromDB(tx, bMeta.Name)
			Expect(err).To(BeNil())
			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())

			Expect(actualMeta.IFKs).To(HaveLen(1))
			Expect(actualMeta.IFKs[0].OnDelete).To(Equal("RESTRICT"))

			//assert meta
			bMeta, _, err = metaStore.Get(bMeta.Name, true)
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(description.OnDeleteRestrict))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'RESTRICT' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields, description.Field{
				Name:     "a",
				LinkMeta: aMetaDescription.Name,
				Type:     description.FieldTypeObject,
				LinkType: description.LinkTypeInner,
				OnDelete: "setDefault",
				Optional: false,
			})
			aMeta, err := metaStore.NewMeta(aMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(aMeta)
			Expect(err).To(BeNil())

			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			//assert schema
			actualMeta, err := pg.MetaDDLFromDB(tx, bMeta.Name)
			Expect(err).To(BeNil())
			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())

			Expect(actualMeta.IFKs).To(HaveLen(1))
			Expect(actualMeta.IFKs[0].OnDelete).To(Equal("SET DEFAULT"))

			//assert meta
			bMeta, _, err = metaStore.Get(bMeta.Name, true)
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(description.OnDeleteSetDefault))
			Expect(err).To(BeNil())
		})
	})

	It("Keeps m2m outer fields on meta update", func() {
		childMetaDescription := GetBaseMetaData(utils.RandomString(8))
		childMeta, _ := metaStore.NewMeta(childMetaDescription)
		err := metaStore.Create(childMeta)
		Expect(err).To(BeNil())

		parentMetaDescription := GetBaseMetaData(utils.RandomString(8))
		parentMetaDescription.Fields = append(parentMetaDescription.Fields, description.Field{
			Name:     "child",
			LinkMeta: childMeta.Name,
			Type:     description.FieldTypeObjects,
			LinkType: description.LinkTypeInner,
			Optional: false,
		})
		parentMeta, err := metaStore.NewMeta(parentMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(parentMeta)
		Expect(err).To(BeNil())

		updatedMetaDescription := GetBaseMetaData(childMeta.Name)
		updatedMeta, _ := metaStore.NewMeta(updatedMetaDescription)
		metaStore.Update(childMeta.Name, updatedMeta, true)

		testMeta, _, _ := metaStore.Get(childMeta.Name, false)
		Expect(testMeta.FindField(fmt.Sprintf("%s__%s_set", parentMeta.Name, childMeta.Name))).NotTo(BeNil())
	})
})