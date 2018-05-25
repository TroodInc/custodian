package meta_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"server/meta"
)

var _ = Describe("MetaStore flushes all objects", func() {
	databaseConnectionOptions := "host=localhost dbname=custodian sslmode=disable"
	syncer, _ := pg.NewSyncer(databaseConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	Describe("Once object is created", func() {
		metaDescription := meta.MetaDescription{
			Name: "person",
			Key:  "id",
			Cas:  false,
			Fields: []meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]string{
						"func": "nextval",
					},
				}, {
					Name:     "name",
					Type:     meta.FieldTypeString,
					Optional: false,
				}, {
					Name:     "gender",
					Type:     meta.FieldTypeString,
					Optional: true,
				},
			},
		}
		meta, _ := metaStore.NewMeta(&metaDescription)
		metaStore.Create(meta)

		It("MetaStore returns a list containing one object", func() {
			metaList, _, _ := metaStore.List()
			Expect(*metaList).To(HaveLen(1))
		})

		It("but after flush is done it returns empty list", func() {
			metaStore.Flush()
			metaList, _, _ := metaStore.List()
			Expect(*metaList).To(HaveLen(0))
		})
	})
})
