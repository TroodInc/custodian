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
	metaStore := object.NewStore(object.NewFileMetaDriver("./"), syncer)

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	It("Changes field`s 'optional' attribute from 'false' to 'true' with corresponding database column altering", func() {
		Describe("Having an object with required field", func() {
			//create meta
			metaDescription := object.MetaDescription{
				Name: "person",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name: "id",
						Type: object.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					}, {
						Name:     "name",
						Type:     object.FieldTypeString,
						Optional: false,
					},
				},
			}
			objectMeta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())

			err = metaStore.Create(objectMeta)
			Expect(err).To(BeNil())

			Describe("this field is specified as optional and object is updated", func() {
				metaDescription = object.MetaDescription{
					Name: "person",
					Key:  "id",
					Cas:  false,
					Fields: []object.Field{
						{
							Name: "id",
							Type: object.FieldTypeNumber,
							Def: map[string]interface{}{
								"func": "nextval",
							},
							Optional: true,
						}, {
							Name:     "name",
							Type:     object.FieldTypeString,
							Optional: true,
						},
					},
				}
				objectMeta, err := metaStore.NewMeta(&metaDescription)
				Expect(err).To(BeNil())

				_, err = metaStore.Update(objectMeta.Name, objectMeta, true, true)
				Expect(err).To(BeNil())

				db, err := sql.Open("postgres", appConfig.DbConnectionOptions)
				Expect(err).To(BeNil())

				tx, err := db.Begin()
				Expect(err).To(BeNil())

				defer tx.Rollback()

				metaDdl, err := pg.MetaDDLFromDB(tx, objectMeta.Name)
				Expect(err).To(BeNil())
				Expect(metaDdl.Columns[1].Optional).To(BeTrue())
			})
		})

	})

	It("Changes field`s 'optional' attribute from 'true' to 'false' with corresponding database column altering", func() {
		Describe("Having an object with required field", func() {
			//create meta
			metaDescription := object.MetaDescription{
				Name: "person",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name: "id",
						Type: object.FieldTypeNumber,
						Def: map[string]interface{}{
							"func": "nextval",
						},
						Optional: true,
					}, {
						Name:     "name",
						Type:     object.FieldTypeString,
						Optional: true,
					},
				},
			}
			objectMeta, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectMeta)
			Expect(err).To(BeNil())

			Describe("this field is specified as optional and object is updated", func() {
				metaDescription = object.MetaDescription{
					Name: "person",
					Key:  "id",
					Cas:  false,
					Fields: []object.Field{
						{
							Name: "id",
							Type: object.FieldTypeNumber,
							Def: map[string]interface{}{
								"func": "nextval",
							},
							Optional: true,
						}, {
							Name:     "name",
							Type:     object.FieldTypeString,
							Optional: false,
						},
					},
				}
				objectMeta, err := metaStore.NewMeta(&metaDescription)
				Expect(err).To(BeNil())
				_, err = metaStore.Update(objectMeta.Name, objectMeta, true, true)
				Expect(err).To(BeNil())

				db, err := sql.Open("postgres", appConfig.DbConnectionOptions)
				Expect(err).To(BeNil())

				tx, err := db.Begin()
				Expect(err).To(BeNil())

				metaDdl, err := pg.MetaDDLFromDB(tx, objectMeta.Name)
				Expect(err).To(BeNil())
				Expect(metaDdl.Columns[1].Optional).To(BeFalse())
			})
		})

	})
})
