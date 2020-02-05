package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/auth"
	"server/data"
	"server/data/errors"

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
	fileMetaTransactionManager := &transactions.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(transactions.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	havingAMetaDescription := func() *meta.Meta {
		By("Having A meta")
		aMetaDescription := meta.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
			},
		}
		return &aMetaDescription
	}

	havingBMetaDescription := func() *meta.Meta {
		By("Having B referencing A with 'setNull' strategy")
		bMetaDescription := meta.Meta{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []*meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "a",
					Type:     meta.FieldTypeObject,
					LinkType: meta.LinkTypeInner,
					LinkMeta: "a",
					OnDelete: "setNull",
					Optional: false,
				},
			},
		}
		return &bMetaDescription
	}

	havingCMetaDescription := func() *meta.Meta {
		By("Having C meta referencing B meta with 'cascade' strategy")
		cMetaDescription := meta.Meta{
			Name: "c",
			Key:  "id",
			Cas:  false,
			Fields: []*meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				},
				{
					Name:     "b",
					Type:     meta.FieldTypeObject,
					LinkType: meta.LinkTypeInner,
					LinkMeta: "b",
					OnDelete: "cascade",
				},
			},
		}
		return &cMetaDescription
	}

	createMeta := func(metaDescription *meta.Meta) *meta.Meta {
		metaObj, err := metaStore.NewMeta(metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	It("builds record tree with 'SetNull' strategy", func() {

		aMeta := createMeta(havingAMetaDescription())
		bMetaDescription := havingBMetaDescription()
		bMetaDescription.Fields[1].OnDelete = meta.OnDeleteSetNull.ToVerbose()
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
		bMetaDescription := havingBMetaDescription()
		bMetaDescription.Fields[1].OnDelete = meta.OnDeleteRestrict.ToVerbose()
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
		globalTransaction, err := globalTransactionManager.BeginTransaction()
		_, err = new(data.RecordRemovalTreeBuilder).Extract(aRecord, dataProcessor, globalTransaction.DbTransaction)
		globalTransactionManager.CommitTransaction(globalTransaction)

		By("It should return error")
		Expect(err).NotTo(BeNil())
		Expect(err.(*errors.RemovalError).MetaName).To(Equal(aMeta.Name))
	})

	It("builds record tree with 'cascade' strategy", func() {

		aMeta := createMeta(havingAMetaDescription())
		bMetaDescription := havingBMetaDescription()
		bMetaDescription.Fields[1].OnDelete = meta.OnDeleteCascade.ToVerbose()
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
