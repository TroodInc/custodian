package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
	"server/pg"
	"database/sql"
	"utils"
)

var _ = Describe("Store", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	It("Changes field`s 'optional' attribute from 'false' to 'true' with corresponding database column altering", func() {
		Describe("Having an object with required field", func() {
			//create meta
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
						Optional: false,
					}, {
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: false,
					},
				},
			}
			objectMeta, _ := metaStore.NewMeta(&metaDescription)
			err := metaStore.Create(objectMeta)
			Expect(err).To(BeNil())

			Describe("this field is specified as optional and object is updated", func() {
				metaDescription = meta.MetaDescription{
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
							Optional: false,
						}, {
							Name:     "name",
							Type:     meta.FieldTypeString,
							Optional: true,
						},
					},
				}
				objectMeta, _ := metaStore.NewMeta(&metaDescription)
				_, err := metaStore.Update(objectMeta.Name, objectMeta)
				Expect(err).To(BeNil())

				db, err := sql.Open("postgres", appConfig.DbConnectionOptions)

				metaDdl, err := pg.MetaDDLFromDB(db, objectMeta.Name)
				Expect(err).To(BeNil())
				Expect(metaDdl.Columns[1].Optional).To(BeTrue())
			})
		})

	})

	It("Changes field`s 'optional' attribute from 'true' to 'false' with corresponding database column altering", func() {
		Describe("Having an object with required field", func() {
			//create meta
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
						Optional: false,
					}, {
						Name:     "name",
						Type:     meta.FieldTypeString,
						Optional: true,
					},
				},
			}
			objectMeta, _ := metaStore.NewMeta(&metaDescription)
			err := metaStore.Create(objectMeta)
			Expect(err).To(BeNil())

			Describe("this field is specified as optional and object is updated", func() {
				metaDescription = meta.MetaDescription{
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
							Optional: false,
						}, {
							Name:     "name",
							Type:     meta.FieldTypeString,
							Optional: false,
						},
					},
				}
				objectMeta, _ := metaStore.NewMeta(&metaDescription)
				_, err := metaStore.Update(objectMeta.Name, objectMeta)
				Expect(err).To(BeNil())

				db, err := sql.Open("postgres", appConfig.DbConnectionOptions)

				metaDdl, err := pg.MetaDDLFromDB(db, objectMeta.Name)
				Expect(err).To(BeNil())
				Expect(metaDdl.Columns[1].Optional).To(BeFalse())
			})
		})

	})
})
