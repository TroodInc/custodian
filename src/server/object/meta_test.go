package object

import (
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/errors"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = FDescribe("The PG MetaStore", func() {
	fileMetaDriver := transactions.NewFileMetaDescriptionSyncer("./")
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(fileMetaDriver)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := NewStore(fileMetaDriver, syncer, globalTransactionManager)

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

				metaList := metaStore.List()
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
			bMetaDescription.Fields = append(bMetaDescription.Fields, &Field{
				Name:     "a_fk",
				Type:     FieldTypeObject,
				Optional: true,
				LinkType: LinkTypeInner,
				LinkMeta: aMeta,
			})
			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			aMetaDescription.Fields = append(aMetaDescription.Fields, &Field{
				Name:           "b_set",
				Type:           FieldTypeObject,
				Optional:       true,
				LinkType:       LinkTypeOuter,
				LinkMeta:       bMeta,
				OuterLinkField: bMeta.FindField("a_fk"),
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
			aMeta, err := metaStore.NewMeta(aMetaDescription)/*
			Expect(err).To(BeNil())*/
			metaStore.Create(aMeta)

			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields,  &Field{
				Name:     "a_fk",
				Type:     FieldTypeObject,
				Optional: true,
				LinkType: LinkTypeInner,
				LinkMeta: aMeta,
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
			bMetaDescription.Fields = append(bMetaDescription.Fields,  &Field{
				Name:     "a_fk",
				Type:     FieldTypeObject,
				Optional: true,
				LinkType: LinkTypeInner,
				LinkMeta: aMeta,
			})
			bMeta, err := metaStore.NewMeta(bMetaDescription)
			Expect(err).To(BeNil())
			metaStore.Create(bMeta)
			Expect(err).To(BeNil())

			aMetaDescription.Fields = append(aMetaDescription.Fields,  &Field{
				Name:           "b_set",
				Type:           FieldTypeObject,
				Optional:       true,
				LinkType:       LinkTypeOuter,
				LinkMeta:       bMeta,
				OuterLinkField: bMeta.FindField("a_fk"),
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
			metaDescription.Fields = append(metaDescription.Fields, []*Field{
				{
					Name:     "name",
					Type:     FieldTypeString,
					Optional: false,
				}, {
					Name:     "name",
					Type:     FieldTypeString,
					Optional: true,
				},
			}...)
			Context("When 'NewMeta' method is called it should return error", func() {
				_, err := metaStore.NewMeta(metaDescription)
				Expect(err).To(Not(BeNil()))
				Expect(err).To(Equal(
					errors.NewValidationError("", "Object contains duplicated field 'name'", nil)),
				)
			})
		})
	})

	It("can change field type of existing object", func() {
		By("having an existing object with string field")
		metaDescription := GetBaseMetaData(utils.RandomString(8))
		metaDescription.Fields = append(metaDescription.Fields,  &Field{
			Name:     "name",
			Type:     FieldTypeNumber,
			Optional: false,
		})
		metaObj, err := metaStore.NewMeta(metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		Context("when object is updated with modified field`s type", func() {
			metaDescription = GetBaseMetaData(metaDescription.Name)
			metaDescription.Fields = append(metaDescription.Fields, &Field{
				Name:     "name",
				Type:     FieldTypeString,
				Optional: false,
			})
			metaObj, err := metaStore.NewMeta(metaDescription)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(metaObj.Name, metaObj, true)
			Expect(err).To(BeNil())

			globalTransaction, err := globalTransactionManager.BeginTransaction()
			tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
			Expect(err).To(BeNil())

			actualMeta, err := pg.MetaDDLFromDB(tx, metaObj.Name)
			Expect(err).To(BeNil())
			err = globalTransactionManager.CommitTransaction(globalTransaction)
			Expect(err).To(BeNil())

			Expect(err).To(BeNil())
			Expect(actualMeta.Columns[1].Typ).To(Equal(FieldTypeString))
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'CASCADE' by default", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B referencing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields,  &Field{
				Name:     "a",
				LinkMeta: aMetaDescription,
				Type:     FieldTypeObject,
				LinkType: LinkTypeInner,
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

			globalTransaction, err := globalTransactionManager.BeginTransaction()
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
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(OnDeleteCascade))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'CASCADE' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields,  &Field{
				Name:     "a",
				LinkMeta: aMetaDescription,
				Type:     FieldTypeObject,
				LinkType: LinkTypeInner,
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

			globalTransaction, err := globalTransactionManager.BeginTransaction()
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
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(OnDeleteCascade))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'SET NULL' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields,  &Field{
				Name:     "a",
				LinkMeta: aMetaDescription,
				Type:     FieldTypeObject,
				LinkType: LinkTypeInner,
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

			globalTransaction, err := globalTransactionManager.BeginTransaction()
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
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(OnDeleteSetNull))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'RESTRICT' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields,  &Field{
				Name:     "a",
				LinkMeta: aMetaDescription,
				Type:     FieldTypeObject,
				LinkType: LinkTypeInner,
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

			globalTransaction, err := globalTransactionManager.BeginTransaction()
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
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(OnDeleteRestrict))
			Expect(err).To(BeNil())
		})
	})

	It("creates inner link with 'on_delete' behavior defined as 'RESTRICT' when manually specified", func() {
		By("having an object A")
		aMetaDescription := GetBaseMetaData(utils.RandomString(8))
		By("and having an object B reversing A")
		Context("when object is updated with modified field`s type", func() {
			bMetaDescription := GetBaseMetaData(utils.RandomString(8))
			bMetaDescription.Fields = append(bMetaDescription.Fields,  &Field{
				Name:     "a",
				LinkMeta: aMetaDescription,
				Type:     FieldTypeObject,
				LinkType: LinkTypeInner,
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

			globalTransaction, err := globalTransactionManager.BeginTransaction()
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
			Expect(*bMeta.FindField("a").OnDeleteStrategy()).To(Equal(OnDeleteSetDefault))
			Expect(err).To(BeNil())
		})
	})
})
