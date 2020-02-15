package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/data"
	"server/object"
	"server/object/driver"
	"server/object/meta"

	"utils"
)

var _ = Describe("Node", func() {
	appConfig := utils.GetConfig()

	driver := driver.NewJsonDriver(appConfig.DbConnectionUrl, "./")
	metaStore  := object.NewStore(driver)

	AfterEach(func() {
		metaStore.Flush()
	})

	It("can fill child nodes with circular dependency", func() {

		Describe("Having three objects with mediated circular dependency", func() {
			objectA := object.GetBaseMetaData(utils.RandomString(8))
			objectAMeta, err := metaStore.NewMeta(objectA)
			Expect(err).To(BeNil())
			metaStore.Create(objectAMeta)

			objectB := object.GetBaseMetaData(utils.RandomString(8))
			objectB.AddField(&meta.Field{
				Name:     "a",
				Type:     meta.FieldTypeObject,
				Optional: true,
				LinkType: meta.LinkTypeInner,
				LinkMeta: objectAMeta,
			})
			objectBMeta, err := metaStore.NewMeta(objectB)
			Expect(err).To(BeNil())
			metaStore.Create(objectBMeta)

			objectC := object.GetBaseMetaData(utils.RandomString(8))
			objectC.AddField(&meta.Field{
				Name:     "b",
				Type:     meta.FieldTypeObject,
				Optional: true,
				LinkType: meta.LinkTypeInner,
				LinkMeta: objectBMeta,
			})
			objectCMeta, err := metaStore.NewMeta(objectC)
			Expect(err).To(BeNil())
			metaStore.Create(objectCMeta)

			objectA = object.GetBaseMetaData(utils.RandomString(8))
			objectA.AddField(&meta.Field{
				Name:     "c",
				Type:     meta.FieldTypeObject,
				Optional: true,
				LinkType: meta.LinkTypeInner,
				LinkMeta: objectCMeta,
			})
			objectAMeta, err = metaStore.NewMeta(objectA)
			Expect(err).To(BeNil())
			metaStore.Update(objectAMeta)
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
