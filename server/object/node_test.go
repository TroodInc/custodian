package object_test

import (
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Node", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := object.NewStore(metaDescriptionSyncer, syncer, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("can fill child nodes with circular dependency", func() {

		testObjAName := fmt.Sprintf("%s_a", utils.RandomString(8))
		testObjBName := fmt.Sprintf("%s_b", utils.RandomString(8))
		testObjCName := fmt.Sprintf("%s_c", utils.RandomString(8))

		Describe("Having three objects with mediated circular dependency", func() {
			objectA := description.MetaDescription{
				Name: testObjAName,
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
				Name: testObjBName,
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id",
						Type: description.FieldTypeString,
					},
					{
						Name:     testObjAName,
						Type:     description.FieldTypeObject,
						Optional: true,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjAName,
					},
				},
			}
			objectBMeta, err := metaStore.NewMeta(&objectB)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectBMeta)
			Expect(err).To(BeNil())

			objectC := description.MetaDescription{
				Name: testObjCName,
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id",
						Type: description.FieldTypeString,
					},
					{
						Name:     testObjBName,
						Type:     description.FieldTypeObject,
						Optional: true,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjBName,
					},
				},
			}
			objectCMeta, err := metaStore.NewMeta(&objectC)
			Expect(err).To(BeNil())
			err = metaStore.Create(objectCMeta)
			Expect(err).To(BeNil())

			objectA = description.MetaDescription{
				Name: testObjAName,
				Key:  "id",
				Cas:  false,
				Fields: []description.Field{
					{
						Name: "id",
						Type: description.FieldTypeString,
					},
					{
						Name:     testObjCName,
						Type:     description.FieldTypeObject,
						Optional: true,
						LinkType: description.LinkTypeInner,
						LinkMeta: testObjCName,
					},
				},
			}
			objectAMeta, err = metaStore.NewMeta(&objectA)
			Expect(err).To(BeNil())
			_, err = metaStore.Update(objectA.Name, objectAMeta, true)
			Expect(err).To(BeNil())

			Describe("", func() {

				node := &object.Node{
					KeyField:   objectAMeta.Key,
					Meta:       objectAMeta,
					ChildNodes: *object.NewChildNodes(),
					Depth:      1,
					OnlyLink:   false,
					Parent:     nil,
				}
				node.RecursivelyFillChildNodes(100, description.FieldModeRetrieve)
				Expect(node.ChildNodes.Nodes()[testObjCName].ChildNodes.Nodes()[testObjBName].ChildNodes.Nodes()[testObjAName].ChildNodes.Nodes()).To(HaveLen(0))
			})
		})
	})
})
