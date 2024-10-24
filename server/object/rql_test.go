package object

import (
	"custodian/server/object/description"

	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	rqlParser "github.com/Q-CIS-DEV/go-rql-parser"
)

var _ = Describe("RQL test", func() {
	appConfig := utils.GetConfig()
	db, _ := NewDbConnection(appConfig.DbConnectionUrl)

	dbTransactionManager := NewPgDbTransactionManager(db)

	metaDescriptionSyncer := NewPgMetaDescriptionSyncer(dbTransactionManager, NewCache(), db)
	metaStore := NewStore(metaDescriptionSyncer, dbTransactionManager)

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

	dataNode := &Node{
		KeyField:   meta.Key,
		Meta:       meta,
		ChildNodes: *NewChildNodes(),
		Depth:      1,
		OnlyLink:   false,
		Parent:     nil,
		Type:       NodeTypeRegular,
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
