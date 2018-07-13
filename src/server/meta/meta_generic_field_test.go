package meta_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"server/meta"
	"utils"
	"database/sql"
)

var _ = FDescribe("Generic field", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	It("can create object with generic field", func() {
		By("having two objects: A and B")
		aMetaDescription := meta.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		bMetaDescription := meta.MetaDescription{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("and object C, containing generic inner field")

		cMetaDescription := meta.MetaDescription{
			Name: "c",
			Key:  "id",
			Cas:  false,
			Fields: []meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target",
					Type:         meta.FieldTypeGeneric,
					LinkType:     meta.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name, bMetaObj.Name},
					Optional:     false,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		db, err := sql.Open("postgres", appConfig.DbConnectionOptions)
		tx, err := db.Begin()
		Expect(err).To(BeNil())

		tableName := pg.GetTableName(metaObj)

		reverser, err := pg.NewReverser(tx, tableName)
		columns := make([]pg.Column, 0)
		pk := ""
		reverser.Columns(&columns, &pk)
		Expect(columns).To(HaveLen(3))

	})

	It("Validates linked metas", func() {
		By("having an object A, referencing non-existing object B")

		cMetaDescription := meta.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target",
					Type:         meta.FieldTypeGeneric,
					LinkType:     meta.LinkTypeInner,
					LinkMetaList: []string{"b"},
					Optional:     false,
				},
			},
		}
		By("Meta should not be created")
		_, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(Not(BeNil()))
	})
})
