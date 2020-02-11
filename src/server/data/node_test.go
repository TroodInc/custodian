package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/data"
	"server/object"

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
	metaDescriptionSyncer := transactions.NewFileMetaDescriptionSyncer("./")
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)

	metaStore := object.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can fill child nodes with circular dependency", func() {

		Describe("Having three objects with mediated circular dependency", func() {
			objectA := object.Meta{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
					{
						Name: "id",
						Type: object.FieldTypeString,
					},
				},
			}
			objectAMeta, err := metaStore.NewMeta(&objectA)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectAMeta)
			Expect(err).To(BeNil())

			objectB := object.Meta{
				Name: "b",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
					{
						Name: "id",
						Type: object.FieldTypeString,
					},
					{
						Name:     "a",
						Type:     object.FieldTypeObject,
						Optional: true,
						LinkType: object.LinkTypeInner,
						LinkMeta: objectAMeta,
					},
				},
			}
			objectBMeta, err := metaStore.NewMeta(&objectB)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectBMeta)
			Expect(err).To(BeNil())

			objectC := object.Meta{
				Name: "c",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
					{
						Name: "id",
						Type: object.FieldTypeString,
					},
					{
						Name:     "b",
						Type:     object.FieldTypeObject,
						Optional: true,
						LinkType: object.LinkTypeInner,
						LinkMeta: objectBMeta,
					},
				},
			}
			objectCMeta, err := metaStore.NewMeta(&objectC)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectCMeta)
			Expect(err).To(BeNil())

			objectA = object.Meta{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []*object.Field{
					{
						Name: "id",
						Type: object.FieldTypeString,
					},
					{
						Name:     "c",
						Type:     object.FieldTypeObject,
						Optional: true,
						LinkType: object.LinkTypeInner,
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
				node.RecursivelyFillChildNodes(100, object.FieldModeRetrieve)
				Expect(node.ChildNodes.Nodes()["c"].ChildNodes.Nodes()["b"].ChildNodes.Nodes()["a"].ChildNodes.Nodes()).To(HaveLen(0))
			})
		})
	})
})
