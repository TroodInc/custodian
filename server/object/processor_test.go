package object_test

import (
	"custodian/server/object"
	"custodian/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"custodian/server/auth"
	"custodian/server/object/description"
)

var _ = Describe("Store", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)

	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache(), db)

	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)
	dataProcessor, _ := object.NewProcessor(metaStore, dbTransactionManager)

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

		var userId2 int = 101
		recordData2 := map[string]interface{}{
			"name": "Test",
			"id":   1001,
		}
		record2, _ := dataProcessor.CreateRecord(meta.Name, recordData2, auth.User{Id: userId2})
		Expect(record2.Data["profile"]).To(Equal(float64(userId2)))
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
