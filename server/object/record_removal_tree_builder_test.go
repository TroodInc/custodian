package object_test

import (
	"custodian/server/auth"
	"custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/errors"

	"custodian/utils"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Record tree extractor", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache())
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)
	dataProcessor, _ := object.NewProcessor(metaStore, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	testObjAName := utils.RandomString(8)
	testObjBName := utils.RandomString(8)
	testObjСName := utils.RandomString(8)
	testObjBSetName := fmt.Sprintf("%s_set", testObjBName)
	testObjCSetName := fmt.Sprintf("%s_set", testObjСName)

	havingAMetaDescription := func() *description.MetaDescription {
		By("Having A meta")
		aMetaDescription := description.MetaDescription{
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
					Optional: true,
				},
			},
		}
		return &aMetaDescription
	}

	havingBMetaDescription := func() *description.MetaDescription {
		By("Having B referencing A with 'setNull' strategy")
		bMetaDescription := description.MetaDescription{
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
					Optional: true,
				},
				{
					Name:     testObjAName,
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjAName,
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
			Name: testObjСName,
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
					Name:     testObjBName,
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjBName,
					OnDelete: "cascade",
				},
			},
		}
		return &cMetaDescription
	}

	createMeta := func(metaDescription *description.MetaDescription) *object.Meta {
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

		bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{testObjAName: aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(cMeta.Name, map[string]interface{}{testObjBName: bRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aMeta, _, err = metaStore.Get(aMeta.Name, true)
		Expect(err).To(BeNil())

		By("Building removal node for A record")
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		recordNode, err := new(object.RecordRemovalTreeBuilder).Extract(aRecord, dataProcessor, globalTransaction)
		globalTransaction.Commit()

		By("It should only contain B record marked with 'setNull' strategy")
		Expect(err).To(BeNil())
		Expect(recordNode).NotTo(BeNil())
		Expect(recordNode.Children).To(HaveLen(1))
		Expect(recordNode.Children).To(HaveKey(testObjBSetName))
		Expect(recordNode.Children[testObjBSetName]).To(HaveLen(1))
		Expect(recordNode.Children[testObjBSetName][0].Children).To(BeEmpty())
		Expect(*recordNode.Children[testObjBSetName][0].OnDeleteStrategy).To(Equal(description.OnDeleteSetNull))
	})

	It("returns error with 'restrict' strategy", func() {

		aMeta := createMeta(havingAMetaDescription())
		bMetaDescription := havingBMetaDescription()
		bMetaDescription.Fields[1].OnDelete = description.OnDeleteRestrict.ToVerbose()
		bMeta := createMeta(bMetaDescription)
		cMeta := createMeta(havingCMetaDescription())

		aRecord, err := dataProcessor.CreateRecord(aMeta.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{testObjAName: aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(cMeta.Name, map[string]interface{}{testObjBName: bRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aMeta, _, err = metaStore.Get(aMeta.Name, true)
		Expect(err).To(BeNil())

		By("Building removal node for A record")
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		_, err = new(object.RecordRemovalTreeBuilder).Extract(aRecord, dataProcessor, globalTransaction)
		if err != nil {
			globalTransaction.Rollback()
		}

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

		bRecord, err := dataProcessor.CreateRecord(bMeta.Name, map[string]interface{}{testObjAName: aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(cMeta.Name, map[string]interface{}{testObjBName: bRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())

		aMeta, _, err = metaStore.Get(aMeta.Name, true)
		Expect(err).To(BeNil())

		By("Building removal node for A record")
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		recordNode, err := new(object.RecordRemovalTreeBuilder).Extract(aRecord, dataProcessor, globalTransaction)
		globalTransaction.Commit()

		By("It should contain B record marked with 'cascade' strategy, which contains C record containing 'cascade' strategy")
		Expect(err).To(BeNil())
		Expect(recordNode).NotTo(BeNil())
		Expect(recordNode.Children).To(HaveLen(1))
		Expect(recordNode.Children).To(HaveKey(testObjBSetName))
		Expect(recordNode.Children[testObjBSetName]).To(HaveLen(1))
		Expect(*recordNode.Children[testObjBSetName][0].OnDeleteStrategy).To(Equal(description.OnDeleteCascade))
		Expect(recordNode.Children[testObjBSetName][0].Children).To(HaveLen(1))
		Expect(recordNode.Children[testObjBSetName][0].Children).To(HaveKey(testObjCSetName))
		Expect(recordNode.Children[testObjBSetName][0].Children[testObjCSetName]).To(HaveLen(1))
		Expect(*(recordNode.Children[testObjBSetName][0].Children[testObjCSetName][0].OnDeleteStrategy)).To(Equal(description.OnDeleteCascade))
		Expect(recordNode.Children[testObjBSetName][0].Children[testObjCSetName][0].Children).To(BeEmpty())
	})
})
