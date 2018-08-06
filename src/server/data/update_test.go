package data_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/meta"
	"server/pg"
	"server/data"
	"server/auth"
	"strconv"
	"utils"
)

var _ = Describe("Data", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := meta.NewStore(meta.NewFileMetaDriver("./"), syncer)

	dataManager, _ := syncer.NewDataManager()
	dataProcessor, _ := data.NewProcessor(metaStore, dataManager)

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	It("Can update records containing reserved words", func() {
		Context("having an object named by reserved word and containing field named by reserved word", func() {

			metaDescription := meta.MetaDescription{
				Name: "order",
				Key:  "order",
				Cas:  false,
				Fields: []meta.Field{
					{
						Name:     "order",
						Type:     meta.FieldTypeNumber,
						Optional: true,
						Def: map[string]interface{}{
							"func": "nextval",
						},
					},
					{
						Name: "select",
						Type: meta.FieldTypeString,
					},
				},
			}
			metaObj, err := metaStore.NewMeta(&metaDescription)
			Expect(err).To(BeNil())
			err = metaStore.Create(metaObj)
			Expect(err).To(BeNil())

			Context("and record of this object", func() {
				record, err := dataProcessor.CreateRecord(metaObj.Name, map[string]interface{}{"select": "some value"}, auth.User{}, true)
				Expect(err).To(BeNil())
				Context("is being updated with values containing reserved word", func() {
					record, err := dataProcessor.UpdateRecord(metaObj.Name, strconv.Itoa(int(record["order"].(float64))), map[string]interface{}{"select": "select"}, auth.User{}, true)
					Expect(err).To(BeNil())
					Expect(record["select"]).To(Equal("select"))
				})

			})

		})

	})

	It("Can perform bulk update", func() {
		By("Having Position object")

		positionMetaDescription := meta.MetaDescription{
			Name: "position",
			Key:  "id",
			Cas:  false,
			Fields: []meta.Field{
				{
					Name:     "id",
					Type:     meta.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name: "name",
					Type: meta.FieldTypeString,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&positionMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and Person object")

		metaDescription := meta.MetaDescription{
			Name: "person",
			Key:  "id",
			Cas:  false,
			Fields: []meta.Field{
				{
					Name:     "id",
					Type:     meta.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "position",
					Type:     meta.FieldTypeObject,
					LinkType: meta.LinkTypeInner,
					LinkMeta: "position",
				},
				{
					Name: "name",
					Type: meta.FieldTypeString,
				},
			},
		}
		metaObj, err = metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and having one record of Position object")
		positionRecord, err := dataProcessor.CreateRecord(positionMetaDescription.Name, map[string]interface{}{"name": "manager"}, auth.User{}, true)
		Expect(err).To(BeNil())

		By("and having two records of Person object")

		records := make([]map[string]interface{}, 2)

		records[0], err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "Ivan", "position": positionRecord["id"]}, auth.User{}, true)
		Expect(err).To(BeNil())

		records[1], err = dataProcessor.CreateRecord(metaDescription.Name, map[string]interface{}{"name": "Vasily", "position": positionRecord["id"]}, auth.User{}, true)
		Expect(err).To(BeNil())

		updatedRecords := make([]map[string]interface{}, 0)

		Context("person records are updated with new name value and new position`s name value as nested object", func() {
			counter := 0
			next := func() (map[string]interface{}, error) {
				if counter < len(records) {
					records[counter]["name"] = "Victor"
					records[counter]["position"] = map[string]interface{}{"id": positionRecord["id"], "name": "sales manager"}
					defer func() { counter += 1 }()
					return records[counter], nil
				}
				return nil, nil
			}

			sink := func(record map[string]interface{}) error {
				updatedRecords = append(updatedRecords, record)
				return nil
			}

			err := dataProcessor.BulkUpdateRecords(metaDescription.Name, next, sink, auth.User{}, true)
			Expect(err).To(BeNil())

			Expect(updatedRecords[0]["name"]).To(Equal("Victor"))

			positionRecord, _ = updatedRecords[0]["position"].(map[string]interface{})
			Expect(positionRecord["name"]).To(Equal("sales manager"))

		})

	})

	It("Can perform update", func() {
		By("Having Position object")

		positionMetaDescription := meta.MetaDescription{
			Name: "position",
			Key:  "id",
			Cas:  false,
			Fields: []meta.Field{
				{
					Name:     "id",
					Type:     meta.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name: "name",
					Type: meta.FieldTypeString,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&positionMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and having one record of Position object")
		recordData, err := dataProcessor.CreateRecord(positionMetaDescription.Name, map[string]interface{}{"name": "manager"}, auth.User{}, true)
		Expect(err).To(BeNil())

		keyValue, _ := recordData["id"].(float64)
		Context("person records are updated with new name value and new position`s name value as nested object", func() {
			recordData["name"] = "sales manager"
			recordData, err := dataProcessor.UpdateRecord(positionMetaDescription.Name, strconv.Itoa(int(keyValue)), recordData, auth.User{}, true)
			Expect(err).To(BeNil())

			Expect(recordData["name"]).To(Equal("sales manager"))

		})

	})

	It("Can update record with null value", func() {
		By("Having A object")

		positionMetaDescription := meta.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []meta.Field{
				{
					Name:     "id",
					Type:     meta.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "name",
					Type:     meta.FieldTypeString,
					Optional: true,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&positionMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("and having one record of A object")
		recordData, err := dataProcessor.CreateRecord(positionMetaDescription.Name, map[string]interface{}{"name": ""}, auth.User{}, true)
		Expect(err).To(BeNil())

		keyValue, _ := recordData["id"].(float64)
		Context("person records are updated with new name value and new position`s name value as nested object", func() {
			recordData["name"] = nil
			recordData, err := dataProcessor.UpdateRecord(positionMetaDescription.Name, strconv.Itoa(int(keyValue)), recordData, auth.User{}, true)
			Expect(err).To(BeNil())

			Expect(recordData["name"]).To(BeNil())
		})

	})
})
