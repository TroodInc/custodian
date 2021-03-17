package object_test

import (
	"custodian/server/object"
	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"custodian/server/auth"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/transactions"
)

var _ = Describe("Store", func() {
	appConfig := utils.GetConfig()
	syncer, _ := object.NewSyncer(appConfig.DbConnectionUrl)

	dataManager, _ := syncer.NewDataManager()
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(dataManager)
	globalTransactionManager := transactions.NewGlobalTransactionManager(dbTransactionManager)
	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(globalTransactionManager)

	metaStore := meta.NewStore(metaDescriptionSyncer, syncer, globalTransactionManager)
	dataProcessor, _ := object.NewProcessor(metaStore, dataManager, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	It("Having an record for person with null value", func() {
		//create meta
		meta := description.MetaDescription{
			Name: "person",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				}, {
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: false,
				}, {
					Name:     "Gender",
					Type:     description.FieldTypeString,
					Optional: true,
				},
			},
		}
		metaDescription, _ := metaStore.NewMeta(&meta)

		err := metaStore.Create(metaDescription)
		Expect(err).To(BeNil())

		//create record
		recordData := map[string]interface{}{
			"name": "Sergey",
		}
		record, _ := dataProcessor.CreateRecord(meta.Name, recordData, auth.User{})
		Expect(record.Data).To(HaveKey("Gender"))
	})

	It("Can set owner of a record", func() {
		//create meta
		meta := description.MetaDescription{
			Name: "test_obj",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
					Optional: true,
				}, {
					Name:     "profile",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "owner",
					},
				}, {
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: false,
				},
			},
		}
		metaDescription, _ := metaStore.NewMeta(&meta)

		err := metaStore.Create(metaDescription)
		Expect(err).To(BeNil())

		//create record
		recordData := map[string]interface{}{
			"name": "Test",
		}
		var userId int = 100
		record, _ := dataProcessor.CreateRecord(meta.Name, recordData, auth.User{Id: userId})
		Expect(record.Data["profile"]).To(Equal(float64(userId)))
	})

	It("Set owner for nested objects", func() {
		childMeta := description.MetaDescription{
			Name: "a_j1298d",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Def:      map[string]interface{}{"func": "nextval"},
					Optional: true,
				}, {
					Name:     "owner",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def:      map[string]interface{}{"func": "owner"},
				}, {
					Name:     "name",
					Type:     description.FieldTypeString,
					Optional: false,
				},
			},
		}
		childObject, _ := metaStore.NewMeta(&childMeta)
		metaStore.Create(childObject)

		parentMeta := description.MetaDescription{
			Name: "b_j1298d",
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Def:      map[string]interface{}{"func": "nextval"},
					Optional: true,
				}, {
					Name:     "child",
					Type:     description.FieldTypeObject,
					LinkMeta: childMeta.Name,
					LinkType: description.LinkTypeInner,
				},
			},
		}

		parentObject, _ := metaStore.NewMeta(&parentMeta)
		metaStore.Create(parentObject)

		//create record
		recordData := map[string]interface{}{
			"child": map[string]interface{}{"name": "Nested object"},
		}
		var userId int = 100
		record, _ := dataProcessor.CreateRecord(parentObject.Name, recordData, auth.User{Id: userId})
		child := record.Data["child"].(map[string]interface{})
		Expect(child["owner"]).To(Equal(float64(userId)))

	})
})
