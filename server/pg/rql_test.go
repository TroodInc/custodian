package pg

import (
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/transactions"
	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	rqlParser "github.com/Q-CIS-DEV/go-rql-parser"

	"custodian/server/data"
)

var _ = Describe("RQL test", func() {
	appConfig := utils.GetConfig()
	syncer, _ := NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	dbTransactionManager := NewPgDbTransactionManager(dataManager)

	globalTransactionManager := transactions.NewGlobalTransactionManager(dbTransactionManager)
	metaDescriptionSyncer := NewPgMetaDescriptionSyncer(globalTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	metaDescription := description.MetaDescription{
		Name: "test",
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
				Name:     "test_field",
				Type:     description.FieldTypeString,
				Optional: true,
			},
			{
				Name:     "camelField",
				Type:     description.FieldTypeString,
				Optional: true,
			},
		},
	}
	meta, _ := metaStore.NewMeta(&metaDescription)

	dataNode := &data.Node{
		KeyField:   meta.Key,
		Meta:       meta,
		ChildNodes: *data.NewChildNodes(),
		Depth:      1,
		OnlyLink:   false,
		Parent:     nil,
		Type:       data.NodeTypeRegular,
	}

	It("handle is_null() operator", func() {
		Context("set to True", func() {
			parser := rqlParser.NewParser()
			rqlNode, _ := parser.Parse("is_null(test_field,true)")
			translator := NewSqlTranslator(rqlNode)

			query, err := translator.query("test", dataNode)

			Expect(err).To(BeNil())
			Expect(query.Where).To(BeEquivalentTo("test.\"test_field\" IS NULL"))
		})

		Context("set to False", func() {
			parser := rqlParser.NewParser()
			rqlNode, _ := parser.Parse("is_null(test_field,false)")
			translator := NewSqlTranslator(rqlNode)

			query, err := translator.query("test", dataNode)

			Expect(err).To(BeNil())
			Expect(query.Where).To(BeEquivalentTo("test.\"test_field\" IS NOT NULL"))
		})

		Context("other non Boolean string", func() {
			parser := rqlParser.NewParser()
			rqlNode, _ := parser.Parse("is_null(test_field,r4nd0m)")
			translator := NewSqlTranslator(rqlNode)

			_, err := translator.query("test", dataNode)

			Expect(err).To(Not(BeNil()))
		})

		Context("with additional rules", func() {
			parser := rqlParser.NewParser()
			rqlNode, _ := parser.Parse("eq(id,1),is_null(test_field,true),is_null(id,false)")
			translator := NewSqlTranslator(rqlNode)

			query, err := translator.query("test", dataNode)

			Expect(err).To(BeNil())
			Expect(query.Where).To(BeEquivalentTo("(test.\"id\" =$1 AND test.\"test_field\" IS NULL AND test.\"id\" IS NOT NULL)"))
		})
	})

	It("handle eq() operator", func() {
		parser := rqlParser.NewParser()
		rqlNode, _ := parser.Parse("eq(id,1)")
		translator := NewSqlTranslator(rqlNode)

		query, err := translator.query("test", dataNode)

		Expect(err).To(BeNil())
		Expect(query.Where).To(BeEquivalentTo("test.\"id\" =$1"))
	})

	It("handle ne() operator", func() {
		parser := rqlParser.NewParser()
		rqlNode, _ := parser.Parse("ne(id,1)")
		translator := NewSqlTranslator(rqlNode)

		query, err := translator.query("test", dataNode)

		Expect(err).To(BeNil())
		Expect(query.Where).To(BeEquivalentTo("test.\"id\" !=$1"))
	})

	It("handle eq() operator", func() {
		parser := rqlParser.NewParser()
		rqlNode, _ := parser.Parse("eq(id,1)")
		translator := NewSqlTranslator(rqlNode)

		query, err := translator.query("test", dataNode)

		Expect(err).To(BeNil())
		Expect(query.Where).To(BeEquivalentTo("test.\"id\" =$1"))
	})

	It("handle eq() operator with camelCase", func() {
		parser := rqlParser.NewParser()
		rqlNode, _ := parser.Parse("eq(camelField,val)")
		translator := NewSqlTranslator(rqlNode)

		query, err := translator.query("test", dataNode)

		Expect(err).To(BeNil())
		Expect(query.Where).To(BeEquivalentTo("test.\"camelField\" =$1"))
	})
})
