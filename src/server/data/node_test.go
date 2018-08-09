package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
	"server/pg"
	"server/data"
	"utils"
)

var _ = Describe("Node", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := object.NewStore(object.NewFileMetaDriver("./"), syncer)

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	It("can fill child nodes with circular dependency", func() {

		Describe("Having three objects with mediated circular dependency", func() {
			objectA := object.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
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

			objectB := object.MetaDescription{
				Name: "b",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name: "id",
						Type: object.FieldTypeString,
					},
					{
						Name:     "a",
						Type:     object.FieldTypeObject,
						Optional: true,
						LinkType: object.LinkTypeInner,
						LinkMeta: "a",
					},
				},
			}
			objectBMeta, err := metaStore.NewMeta(&objectB)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectBMeta)
			Expect(err).To(BeNil())

			objectC := object.MetaDescription{
				Name: "c",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name: "id",
						Type: object.FieldTypeString,
					},
					{
						Name:     "b",
						Type:     object.FieldTypeObject,
						Optional: true,
						LinkType: object.LinkTypeInner,
						LinkMeta: "b",
					},
				},
			}
			objectCMeta, err := metaStore.NewMeta(&objectC)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectCMeta)
			Expect(err).To(BeNil())

			objectA = object.MetaDescription{
				Name: "a",
				Key:  "id",
				Cas:  false,
				Fields: []object.Field{
					{
						Name: "id",
						Type: object.FieldTypeString,
					},
					{
						Name:     "c",
						Type:     object.FieldTypeObject,
						Optional: true,
						LinkType: object.LinkTypeInner,
						LinkMeta: "c",
					},
				},
			}
			objectAMeta, err = metaStore.NewMeta(&objectA)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(objectA.Name, objectAMeta, true, true)
			Expect(err).To(BeNil())

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
