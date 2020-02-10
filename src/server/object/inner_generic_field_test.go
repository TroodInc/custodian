package object

import (
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/meta"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("Inner generic field", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})


	It("can create object with inner generic field", func() {
		By("having two objects: A and B")
		aMetaObj, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("and object C, containing generic inner field")

		cMetaDescription := GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.Fields = append(cMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []*meta.Meta{aMetaObj, bMetaObj},
			Optional:     false,
		})

		metaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		//check database columns
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)
		Expect(err).To(BeNil())

		tableName := pg.GetTableName(metaObj.Name)

		reverser, err := pg.NewReverser(tx, tableName)
		columns := make([]pg.Column, 0)
		pk := ""
		reverser.Columns(&columns, &pk)
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(columns).To(HaveLen(3))
		// check meta fields
		cMeta, _, err := metaStore.Get(cMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(cMeta.Fields).To(HaveLen(2))
		Expect(cMeta.Fields[1].LinkMetaList).To(HaveLen(2))
	})

	It("Validates linked metas", func() {
		By("having an object A, referencing non-existing object B")

		cMetaDescription := GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.Fields = append(cMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{"b"},
			Optional:     false,
		})

		By("MetaDescription should not be created")
		_, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(Not(BeNil()))
	})

	It("can remove generic field from object", func() {

		By("having object A with generic field")
		metaDescription := GetBaseMetaData(utils.RandomString(8))
		metaDescription.Fields = append(metaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{},
			Optional:     false,
		})

		metaObj, err := metaStore.NewMeta(metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		By("when generic field is removed from object and object has been updated")

		metaDescription = GetBaseMetaData(metaDescription.Name)
		metaObj, err = metaStore.NewMeta(metaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(metaObj.Name, metaObj, true)
		Expect(err).To(BeNil())

		//check database columns
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())
		tx := globalTransaction.DbTransaction.Transaction().(*sql.Tx)

		tableName := pg.GetTableName(metaObj.Name)

		reverser, err := pg.NewReverser(tx, tableName)
		columns := make([]pg.Column, 0)
		pk := ""
		reverser.Columns(&columns, &pk)
		globalTransactionManager.CommitTransaction(globalTransaction)
		Expect(columns).To(HaveLen(1))
		Expect(columns[0].Name).To(Equal("id"))
		// check meta fields
		cMeta, _, err := metaStore.Get(metaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(cMeta.Fields).To(HaveLen(1))
		Expect(cMeta.Fields[0].Name).To(Equal("id"))

	})

	It("does not leave orphan links in LinkMetaList on object removal", func() {
		By("having two objects A and B reference by generic field of object C")
		aMetaObj, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		cMetaDescription :=  GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.Fields = append(cMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{aMetaObj.Name, bMetaObj.Name},
			Optional:     false,
		})
		metaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("since object A is deleted, it should be removed from LinkMetaList")

		_, err = metaStore.Remove(aMetaObj.Name, false)
		Expect(err).To(BeNil())

		cMetaObj, _, err := metaStore.Get(cMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(cMetaObj.Fields[1].LinkMetaList).To(HaveLen(1))
	})

	It("can create object with inner generic field", func() {
		By("having object A")

		aMetaObj, err := metaStore.NewMeta(GetBaseMetaData(utils.RandomString(8)))
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		By("and object C, containing generic inner field")

		cMetaDescription :=  GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.Fields = append(cMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{aMetaObj.Name},
			Optional:     false,
		})
		metaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		// check meta fields
		aMeta, _, err := metaStore.Get(aMetaObj.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.Fields[1].Name).To(Equal(cMetaDescription.Name + "_set"))
		Expect(aMeta.Fields[1].LinkType).To(Equal(meta.LinkTypeOuter))
		Expect(aMeta.Fields[1].Type).To(Equal(meta.FieldTypeGeneric))
	})

	It("can create object with inner generic field", func() {
		By("having two objects: A and B")
		aMetaObj, err := metaStore.NewMeta( GetBaseMetaData(utils.RandomString(8)))
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		bMetaObj, err := metaStore.NewMeta( GetBaseMetaData(utils.RandomString(8)))
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("and object C, containing generic inner field")

		cMetaDescription :=  GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.Fields = append(cMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{aMetaObj.Name, bMetaObj.Name},
			Optional:     false,
		})
		metaObj, err := metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		// check A meta fields
		aMeta, _, err := metaStore.Get(cMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))

		By("removing object A from object`s C LinkMetaList")
		cMetaDescription =  GetBaseMetaData(cMetaDescription.Name)
		cMetaDescription.Fields = append(cMetaDescription.Fields, description.Field{
			Name:         "target",
			Type:         description.FieldTypeGeneric,
			LinkType:     description.LinkTypeInner,
			LinkMetaList: []string{bMetaObj.Name},
			Optional:     false,
		})
		metaObj, err = metaStore.NewMeta(cMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(metaObj.Name, metaObj, true)
		Expect(err).To(BeNil())

		//c_set field should be removed from object A
		// check A meta fields
		aMeta, _, err = metaStore.Get(aMetaObj.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(1))
	})
})
