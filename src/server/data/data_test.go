package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
	"server/pg"
	"server/data"
	"server/auth"
)

var _ = Describe("Data", func() {
	databaseConnectionOptions := "host=localhost dbname=custodian sslmode=disable"
	syncer, _ := pg.NewSyncer(databaseConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	It("can create a record containing null value of foreign key field", func() {
		Context("having Reason object", func() {
			reasonMetaDescription := meta.MetaDescription{
				Name: "reason",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeString,
					},
				},
			}
			reasonMetaObj, _ := metaStore.NewMeta(&reasonMetaDescription)
			metaStore.Create(reasonMetaObj)

			Context("and Lead object referencing Reason object", func() {
				leadMetaDescription := meta.MetaDescription{
					Name: "lead",
					Key:  "id",
					Cas:  false,
					Fields: []meta.Field{
						{
							Name: "id",
							Type: meta.FieldTypeNumber,
							Def: map[string]interface{}{
								"func": "nextval",
							},
							Optional: true,
						},
						{
							Name: "name",
							Type: meta.FieldTypeString,
						},
						{
							Name:     "decline_reason",
							Type:     meta.FieldTypeObject,
							Optional: true,
							LinkType: meta.LinkTypeInner,
							LinkMeta: "reason",
						},
					},
				}
				leadMetaObj, _ := metaStore.NewMeta(&leadMetaDescription)
				metaStore.Create(leadMetaObj)
				Context("Lead record with empty reason is created", func() {
					leadData := map[string]interface{}{
						"name": "newLead",
					}
					user := auth.User{}
					record, err := dataProcessor.Put(leadMetaDescription.Name, leadData, user)
					Expect(err).To(BeNil())
					Expect(record).To(HaveKey("decline_reason"))
				})
			})
		})
	})
})
