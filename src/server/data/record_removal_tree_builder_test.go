package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/object/meta"
	"server/pg"
	"utils"
	"server/transactions/file_transaction"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"server/object/description"
	"server/auth"
	"server/data"
	"server/data/errors"
)

var _ = Describe("Record tree extractor", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(meta.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		metaStore.Flush()
	})

	havingAMetaDescription := func() *description.MetaDescription {
		By("Having A meta")
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
		return &aMetaDescription
	}

	havingBMetaDescription := func() *description.MetaDescription {
		By("Having B referencing A with 'setNull' strategy")
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
					LinkType: description.LinkTypeInner,
					LinkMeta: "a",
					OnDelete: "setNull",
					Optional: false,
				},
			},
		}
		return &bMetaDescription
	}

	havingCMetaDescription := func() *description.MetaDescription {
		By("Having C meta referencing B meta with 'cascade' strategy")
		cMetaDescription := description.MetaDescription{
			Name: "c",
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
					Name:     "b",
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: "b",
					OnDelete: "cascade",
				},
			},
		}
		return &cMetaDescription
	}

	createMeta := func(metaDescription *description.MetaDescription) *meta.Meta {
		metaObj, err := metaStore.NewMeta(metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	It("builds record tree with 'SetNull' strategy", func() {

		aMeta := createMeta(havingAMetaDescription())
		bMetaDescription := havingBMetaDescription()
		bMetaDescription.Fields[1].OnDelete = description.OnDeleteSetNull.ToVerbose()
		bMeta := createMeta(bMetaDescription)
		cMeta := createMeta(havingCMetaDescription())

		aRecord, err := dataProcessor.CreateRecord(aMeta.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{"a": aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(cMeta.Name, map[string]interface{}{"b": bRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aMeta, _, err = metaStore.Get(aMeta.Name, true)
		Expect(err).To(BeNil())

		By("Building removal node for A record")
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		recordNode, err := new(data.RecordRemovalTreeBuilder).Extract(aRecord, dataProcessor, globalTransaction.DbTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		By("It should only contain B record marked with 'setNull' strategy")
		Expect(err).To(BeNil())
		Expect(recordNode).NotTo(BeNil())
		Expect(recordNode.Children).To(HaveLen(1))
		Expect(recordNode.Children).To(HaveKey("b_set"))
		Expect(recordNode.Children["b_set"]).To(HaveLen(1))
		Expect(recordNode.Children["b_set"][0].Children).To(BeEmpty())
		Expect(*recordNode.Children["b_set"][0].OnDeleteStrategy).To(Equal(description.OnDeleteSetNull))
	})

	It("returns error with 'restrict' strategy", func() {

		aMeta := createMeta(havingAMetaDescription())
		bMetaDescription := havingBMetaDescription()
		bMetaDescription.Fields[1].OnDelete = description.OnDeleteRestrict.ToVerbose()
		bMeta := createMeta(bMetaDescription)
		cMeta := createMeta(havingCMetaDescription())

		aRecord, err := dataProcessor.CreateRecord(aMeta.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{"a": aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(cMeta.Name, map[string]interface{}{"b": bRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aMeta, _, err = metaStore.Get(aMeta.Name, true)
		Expect(err).To(BeNil())

		By("Building removal node for A record")
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		_, err = new(data.RecordRemovalTreeBuilder).Extract(aRecord, dataProcessor, globalTransaction.DbTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		By("It should return error")
		Expect(err).NotTo(BeNil())
		Expect(err.(*errors.RemovalError).MetaName).To(Equal(aMeta.Name))
	})

	It("builds record tree with 'cascade' strategy", func() {

		aMeta := createMeta(havingAMetaDescription())
		bMetaDescription := havingBMetaDescription()
		bMetaDescription.Fields[1].OnDelete = description.OnDeleteCascade.ToVerbose()
		bMeta := createMeta(bMetaDescription)
		cMeta := createMeta(havingCMetaDescription())

		aRecord, err := dataProcessor.CreateRecord(aMeta.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{"a": aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(cMeta.Name, map[string]interface{}{"b": bRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aMeta, _, err = metaStore.Get(aMeta.Name, true)
		Expect(err).To(BeNil())

		By("Building removal node for A record")
		globalTransaction, err := globalTransactionManager.BeginTransaction(nil)
		recordNode, err := new(data.RecordRemovalTreeBuilder).Extract(aRecord, dataProcessor, globalTransaction.DbTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		By("It should contain B record marked with 'cascade' strategy, which contains C record containing 'cascade' strategy")
		Expect(err).To(BeNil())
		Expect(recordNode).NotTo(BeNil())
		Expect(recordNode.Children).To(HaveLen(1))
		Expect(recordNode.Children).To(HaveKey("b_set"))
		Expect(recordNode.Children["b_set"]).To(HaveLen(1))
		Expect(*recordNode.Children["b_set"][0].OnDeleteStrategy).To(Equal(description.OnDeleteCascade))
		Expect(recordNode.Children["b_set"][0].Children).To(HaveLen(1))
		Expect(recordNode.Children["b_set"][0].Children).To(HaveKey("c_set"))
		Expect(recordNode.Children["b_set"][0].Children["c_set"]).To(HaveLen(1))
		Expect(*(recordNode.Children["b_set"][0].Children["c_set"][0].OnDeleteStrategy)).To(Equal(description.OnDeleteCascade))
		Expect(recordNode.Children["b_set"][0].Children["c_set"][0].Children).To(BeEmpty())
	})
})
