package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"custodian/server/object/meta"
	"custodian/server/object/description"
	"custodian/server/pg"
	"custodian/server/data"
	"custodian/utils"
	pg_transactions "custodian/server/pg/transactions"
	"custodian/server/transactions/file_transaction"
	"custodian/server/transactions"
	"custodian/server/pg_meta"
)

var _ = Describe("Node", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &file_transaction.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	metaDescriptionSyncer := pg_meta.NewPgMetaDescriptionSyncer(dbTransactionManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can fill child nodes with circular dependency", func() {

		Describe("Having three objects with mediated circular dependency", func() {
			objectA := description.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id",
						Type: description.FieldTypeString,
					},
				},
			}
			objectAMeta, err := metaStore.NewMeta(&objectA)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectAMeta)
			Expect(err).To(BeNil())

			objectB := description.MetaDescription{
				Name: "b",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id",
						Type: description.FieldTypeString,
					},
					{
						Name:     "a",
						Type:     description.FieldTypeObject,
						Optional: true,
						LinkType: description.LinkTypeInner,
						LinkMeta: "a",
					},
				},
			}
			objectBMeta, err := metaStore.NewMeta(&objectB)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectBMeta)
			Expect(err).To(BeNil())

			objectC := description.MetaDescription{
				Name: "c",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id",
						Type: description.FieldTypeString,
					},
					{
						Name:     "b",
						Type:     description.FieldTypeObject,
						Optional: true,
						LinkType: description.LinkTypeInner,
						LinkMeta: "b",
					},
				},
			}
			objectCMeta, err := metaStore.NewMeta(&objectC)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectCMeta)
			Expect(err).To(BeNil())

			objectA = description.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id",
						Type: description.FieldTypeString,
					},
					{
						Name:     "c",
						Type:     description.FieldTypeObject,
						Optional: true,
						LinkType: description.LinkTypeInner,
						LinkMeta: "c",
					},
				},
			}
			objectAMeta, err = metaStore.NewMeta(&objectA)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(objectA.Name, objectAMeta, true)
			Expect(err).To(BeNil())

			Describe("", func() {

				node := &data.Node{
					KeyField:   objectAMeta.Key,
					Meta:       objectAMeta,
					ChildNodes: *data.NewChildNodes(),
					Depth:      1,
					OnlyLink:   false,
					Parent:     nil,
				}
				node.RecursivelyFillChildNodes(100, description.FieldModeRetrieve)
				Expect(node.ChildNodes.Nodes()["c"].ChildNodes.Nodes()["b"].ChildNodes.Nodes()["a"].ChildNodes.Nodes()).To(HaveLen(0))
			})
		})
	})
})
