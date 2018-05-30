package meta_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"server/meta"
)

var _ = Describe("The PG MetaStore", func() {

	databaseConnectionOptions := "host=localhost dbname=custodian sslmode=disable"
	syncer, _ := pg.NewSyncer(databaseConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)


	BeforeEach(func() {
		metaStore.Flush()
	})

	It("can flush all objects", func() {
		Context("once object is created", func() {
			metaDescription := meta.MetaDescription{
				Name: "person",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeNumber,
						Def: map[string]interface{}{
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
			Context("and 'flush' method is called", func() {
				metaStore.Flush()
				metaList, _, _ := metaStore.List()
				Expect(*metaList).To(HaveLen(0))
			})
		})
	})
})
