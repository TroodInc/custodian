package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
	"server/pg"
	"server/data"
)

var _ = Describe("Node", func() {

	It("can fill child nodes with circular dependency", func() {
		databaseConnectionOptions := "host=localhost dbname=custodian sslmode=disable"
		syncer, _ := pg.NewSyncer(databaseConnectionOptions)
		metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

		Describe("Having three objects with mediated circular dependency", func() {
			objectA := meta.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeString,
					},
				},
			}
			objectAMeta, _ := metaStore.NewMeta(&objectA)
			metaStore.Create(objectAMeta)

			objectB := meta.MetaDescription{
				Name: "b",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeString,
					},
					{
						Name:     "a",
						Type:     meta.FieldTypeObject,
						Optional: true,
						LinkType: meta.LinkTypeInner,
						LinkMeta: "a",
					},
				},
			}
			objectBMeta, _ := metaStore.NewMeta(&objectB)
			metaStore.Create(objectBMeta)

			objectC := meta.MetaDescription{
				Name: "c",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeString,
					},
					{
						Name:     "b",
						Type:     meta.FieldTypeObject,
						Optional: true,
						LinkType: meta.LinkTypeInner,
						LinkMeta: "b",
					},
				},
			}
			objectCMeta, _ := metaStore.NewMeta(&objectC)
			metaStore.Create(objectCMeta)

			objectA = meta.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name: "id",
						Type: meta.FieldTypeString,
					},
					{
						Name:     "c",
						Type:     meta.FieldTypeObject,
						Optional: true,
						LinkType: meta.LinkTypeInner,
						LinkMeta: "c",
					},
				},
			}
			objectAMeta, _ = metaStore.NewMeta(&objectA)
			metaStore.Update(objectA.Name, objectAMeta)

			Describe("", func() {

				node := &data.Node{
					KeyField:   objectAMeta.Key,
					Meta:       objectAMeta,
					ChildNodes: make(map[string]*data.Node),
					Depth:      1,
					OnlyLink:   false,
					Parent:     nil,
				}
				node.RecursivelyFillChildNodes(100)
				Expect(node.ChildNodes["c"].ChildNodes["b"].ChildNodes["a"].ChildNodes).To(HaveLen(0))
			})
		})

	})

})
