package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/data"

	"server/object/meta"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

var _ = Describe("Node", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	fileMetaTransactionManager := &transactions.FileMetaDescriptionTransactionManager{}
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := meta.NewStore(transactions.NewFileMetaDescriptionSyncer("./"), syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can fill child nodes with circular dependency", func() {

		Describe("Having three objects with mediated circular dependency", func() {
			objectA := meta.Meta{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []*meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeString,
					},
				},
			}
			objectAMeta, err := metaStore.NewMeta(&objectA)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectAMeta)
			Expect(err).To(BeNil())

			objectB := meta.Meta{
				Name: "b",
				Key:  "id",
				Cas:  false,
				Fields: []*meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeString,
					},
					{
						Name:     "a",
						Type:     meta.FieldTypeObject,
						Optional: true,
						LinkType: meta.LinkTypeInner,
						LinkMeta: objectAMeta,
					},
				},
			}
			objectBMeta, err := metaStore.NewMeta(&objectB)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectBMeta)
			Expect(err).To(BeNil())

			objectC := meta.Meta{
				Name: "c",
				Key:  "id",
				Cas:  false,
				Fields: []*meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeString,
					},
					{
						Name:     "b",
						Type:     meta.FieldTypeObject,
						Optional: true,
						LinkType: meta.LinkTypeInner,
						LinkMeta: objectBMeta,
					},
				},
			}
			objectCMeta, err := metaStore.NewMeta(&objectC)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectCMeta)
			Expect(err).To(BeNil())

			objectA = meta.Meta{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []*meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeString,
					},
					{
						Name:     "c",
						Type:     meta.FieldTypeObject,
						Optional: true,
						LinkType: meta.LinkTypeInner,
						LinkMeta: objectCMeta,
					},
				},
			}
			objectAMeta, err = metaStore.NewMeta(&objectA)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(objectA.Name, objectAMeta, true)
			Expect(err).To(BeNil())

			Describe("", func() {

				node := &data.Node{
					KeyField:   objectAMeta.GetKey(),
					Meta:       objectAMeta,
					ChildNodes: *data.NewChildNodes(),
					Depth:      1,
					OnlyLink:   false,
					Parent:     nil,
				}
				node.RecursivelyFillChildNodes(100, meta.FieldModeRetrieve)
				Expect(node.ChildNodes.Nodes()["c"].ChildNodes.Nodes()["b"].ChildNodes.Nodes()["a"].ChildNodes.Nodes()).To(HaveLen(0))
			})
		})
	})
})
