package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/auth"
	"server/data"
	"server/data/errors"
	"server/object"
	"server/object/driver"
	"server/object/meta"

	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("Record tree extractor", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := object.NewStore(driver)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		metaStore.Flush()
	})

	havingAMetaDescription := func() *meta.Meta {
		By("Having A meta")
		return object.GetBaseMetaData(utils.RandomString(8))
	}

	havingBMetaDescription := func(A *meta.Meta) *meta.Meta {
		By("Having B referencing A with 'setNull' strategy")
		bMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		bMetaDescription.AddField(&meta.Field{
			Name:     "a",
			Type:     meta.FieldTypeObject,
			LinkType: meta.LinkTypeInner,
			LinkMeta: A,
			OnDelete: "setNull",
			Optional: false,
		})
		return bMetaDescription
	}

	havingCMetaDescription := func(B *meta.Meta) *meta.Meta {
		By("Having C meta referencing B meta with 'cascade' strategy")
		cMetaDescription := object.GetBaseMetaData(utils.RandomString(8))
		cMetaDescription.AddField(&meta.Field{
			Name:     "b",
			Type:     meta.FieldTypeObject,
			LinkType: meta.LinkTypeInner,
			LinkMeta: B,
			OnDelete: "cascade",
		})
		return cMetaDescription
	}

	createMeta := func(metaDescription *meta.Meta) *meta.Meta {
		metaObj, err := metaStore.NewMeta(metaDescription)
		Expect(err).To(BeNil())
		return metaStore.Create(metaObj)
	}

	It("builds record tree with 'SetNull' strategy", func() {

		aMeta := createMeta(havingAMetaDescription())
		bMetaDescription := havingBMetaDescription(aMeta)
		bMetaDescription.Fields["b"].OnDelete = meta.OnDeleteSetNull.ToVerbose()
		bMeta := createMeta(bMetaDescription)
		cMeta := createMeta(havingCMetaDescription(bMeta))

		aRecord, err := dataProcessor.CreateRecord(aMeta.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{"a": aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(cMeta.Name, map[string]interface{}{"b": bRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aMeta = metaStore.Get(aMeta.Name)
		Expect(err).To(BeNil())

		By("Building removal node for A record")
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		recordNode, err := new(data.RecordRemovalTreeBuilder).Extract(aRecord, dataProcessor, globalTransaction.DbTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		By("It should only contain B record marked with 'setNull' strategy")
		Expect(err).To(BeNil())
		Expect(recordNode).NotTo(BeNil())
		Expect(recordNode.Children).To(HaveLen(1))
		Expect(recordNode.Children).To(HaveKey("b_set"))
		Expect(recordNode.Children["b_set"]).To(HaveLen(1))
		Expect(recordNode.Children["b_set"][0].Children).To(BeEmpty())
		Expect(*recordNode.Children["b_set"][0].OnDeleteStrategy).To(Equal(meta.OnDeleteSetNull))
	})

	It("returns error with 'restrict' strategy", func() {

		aMeta := createMeta(havingAMetaDescription())
		bMetaDescription := havingBMetaDescription(aMeta)
		bMetaDescription.Fields["b"].OnDelete = meta.OnDeleteRestrict.ToVerbose()
		bMeta := createMeta(bMetaDescription)
		cMeta := createMeta(havingCMetaDescription(bMeta))

		aRecord, err := dataProcessor.CreateRecord(aMeta.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{"a": aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(cMeta.Name, map[string]interface{}{"b": bRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aMeta = metaStore.Get(aMeta.Name)
		Expect(err).To(BeNil())

		By("Building removal node for A record")
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		_, err = new(data.RecordRemovalTreeBuilder).Extract(aRecord, dataProcessor, globalTransaction.DbTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		By("It should return error")
		Expect(err).NotTo(BeNil())
		Expect(err.(*errors.RemovalError).MetaName).To(Equal(aMeta.Name))
	})

	It("builds record tree with 'cascade' strategy", func() {

		aMeta := createMeta(havingAMetaDescription())
		bMetaDescription := havingBMetaDescription(aMeta)
		bMetaDescription.Fields["b"].OnDelete = meta.OnDeleteCascade.ToVerbose()
		bMeta := createMeta(bMetaDescription)
		cMeta := createMeta(havingCMetaDescription(bMeta))

		aRecord, err := dataProcessor.CreateRecord(aMeta.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{"a": aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(cMeta.Name, map[string]interface{}{"b": bRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aMeta = metaStore.Get(aMeta.Name)
		Expect(err).To(BeNil())

		By("Building removal node for A record")
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		recordNode, err := new(data.RecordRemovalTreeBuilder).Extract(aRecord, dataProcessor, globalTransaction.DbTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		By("It should contain B record marked with 'cascade' strategy, which contains C record containing 'cascade' strategy")
		Expect(err).To(BeNil())
		Expect(recordNode).NotTo(BeNil())
		Expect(recordNode.Children).To(HaveLen(1))
		Expect(recordNode.Children).To(HaveKey("b_set"))
		Expect(recordNode.Children["b_set"]).To(HaveLen(1))
		Expect(*recordNode.Children["b_set"][0].OnDeleteStrategy).To(Equal(meta.OnDeleteCascade))
		Expect(recordNode.Children["b_set"][0].Children).To(HaveLen(1))
		Expect(recordNode.Children["b_set"][0].Children).To(HaveKey("c_set"))
		Expect(recordNode.Children["b_set"][0].Children["c_set"]).To(HaveLen(1))
		Expect(*(recordNode.Children["b_set"][0].Children["c_set"][0].OnDeleteStrategy)).To(Equal(meta.OnDeleteCascade))
		Expect(recordNode.Children["b_set"][0].Children["c_set"][0].Children).To(BeEmpty())
	})
})
