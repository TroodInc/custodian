package object_test

import (
	"custodian/server/auth"
	"custodian/server/object"
	"custodian/server/object/description"

	"custodian/utils"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RecordSetOperations removal", func() {
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

	It("Can remove records with cascade relation", func() {
		testObjAName := utils.RandomString(8)
		testObjBName := utils.RandomString(8)

		aMetaDescription := description.MetaDescription{
			Name: testObjAName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		//
		bMetaDescription := description.MetaDescription{
			Name: testObjBName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     testObjAName,
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjAName,
					OnDelete: description.OnDeleteCascade.ToVerbose(),
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{testObjAName: aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())
		bKey, _ := bMetaObj.Key.ValueAsString(bRecord.Data["id"])
		aKey, _ := aMetaObj.Key.ValueAsString(aRecord.Data["id"])

		//remove A record
		removedData, err := dataProcessor.RemoveRecord(aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(BeNil())

		//check A record does not exist
		record, _ := dataProcessor.Get(aMetaObj.Name, aKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check B record does not exist
		record, err = dataProcessor.Get(bMetaObj.Name, bKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check removed data tree
		Expect(removedData).NotTo(BeNil())
	})

	It("Can remove record and update child records with 'setNull' relation", func() {
		testObjAName := utils.RandomString(8)
		testObjBName := utils.RandomString(8)
		testObjBSetName := fmt.Sprintf("%s_set", testObjBName)

		aMetaDescription := description.MetaDescription{
			Name: testObjAName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		//
		bMetaDescription := description.MetaDescription{
			Name: testObjBName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     testObjAName,
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjAName,
					Optional: true,
					OnDelete: description.OnDeleteSetNull.ToVerbose(),
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		bRecord, err := dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{testObjAName: aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())
		bKey, _ := bMetaObj.Key.ValueAsString(bRecord.Data["id"])
		aKey, _ := aMetaObj.Key.ValueAsString(aRecord.Data["id"])

		//remove A record
		removedData, err := dataProcessor.RemoveRecord(aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(BeNil())

		//check A record does not exist
		record, _ := dataProcessor.Get(aMetaObj.Name, aKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check B record exists
		record, err = dataProcessor.Get(bMetaObj.Name, bKey, nil, nil, 1, false)
		Expect(record).To(Not(BeNil()))
		Expect(record.Data[testObjAName]).To(BeNil())

		//check removed data tree
		Expect(removedData).NotTo(BeNil())
		Expect(removedData.GetData()).To(Not(HaveKey(testObjBSetName)))
	})

	It("Cannot remove record with 'restrict' relation", func() {
		testObjAName := utils.RandomString(8)
		testObjBName := utils.RandomString(8)

		aMetaDescription := description.MetaDescription{
			Name: testObjAName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		//
		bMetaDescription := description.MetaDescription{
			Name: testObjBName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     testObjAName,
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjAName,
					Optional: true,
					OnDelete: description.OnDeleteRestrict.ToVerbose(),
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		_, err = dataProcessor.CreateRecord(bMetaObj.Name, map[string]interface{}{testObjAName: aRecord.Data["id"]}, auth.User{})
		Expect(err).To(BeNil())
		aKey, _ := aMetaObj.Key.ValueAsString(aRecord.Data["id"])

		//remove A record
		_, err = dataProcessor.RemoveRecord(aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(Not(BeNil()))
	})

	It("Can remove record and update child records with generic relation and 'setNull' strategy", func() {
		testObjAName := utils.RandomString(8)
		testObjBName := utils.RandomString(8)
		testObjBSetName := fmt.Sprintf("%s_set", testObjBName)

		aMetaDescription := description.MetaDescription{
			Name: testObjAName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		//
		bMetaDescription := description.MetaDescription{
			Name: testObjBName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjAName},
					Optional:     true,
					OnDelete:     description.OnDeleteSetNull.ToVerbose(),
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		aKey, _ := aMetaObj.Key.ValueAsString(aRecord.Data["id"])
		bRecord, err := dataProcessor.CreateRecord(
			bMetaObj.Name,
			map[string]interface{}{"target_object": map[string]interface{}{"_object": aMetaObj.Name, "id": aKey}},
			auth.User{},
		)
		Expect(err).To(BeNil())
		bKey, _ := bMetaObj.Key.ValueAsString(bRecord.Data["id"])

		//remove A record
		removedData, err := dataProcessor.RemoveRecord(aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(BeNil())

		//check A record does not exist
		record, _ := dataProcessor.Get(aMetaObj.Name, aKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check B record exists, but generic field value has null value
		record, err = dataProcessor.Get(bMetaObj.Name, bKey, nil, nil, 1, false)
		Expect(record).To(Not(BeNil()))
		Expect(record.Data).To(HaveKey("target_object"))
		Expect(record.Data["target_object"]).To(BeNil())

		//check removed data tree
		Expect(removedData).To(Not(BeNil()))
		Expect(removedData.GetData()).To(Not(HaveKey(testObjBSetName)))
	})

	It("Can remove record and update child records with generic relation and 'cascade' strategy", func() {
		testObjAName := utils.RandomString(8)
		testObjBName := utils.RandomString(8)

		aMetaDescription := description.MetaDescription{
			Name: testObjAName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		aMetaObj, err := metaStore.NewMeta(&aMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(aMetaObj)
		Expect(err).To(BeNil())

		aRecord, err := dataProcessor.CreateRecord(aMetaObj.Name, map[string]interface{}{}, auth.User{})
		Expect(err).To(BeNil())

		//
		bMetaDescription := description.MetaDescription{
			Name: testObjBName,
			Key:  "id",
			Cas:  false,
			Fields: []description.Field{
				{
					Name:     "id",
					Type:     description.FieldTypeNumber,
					Optional: true,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjAName},
					Optional:     true,
					OnDelete:     description.OnDeleteCascade.ToVerbose(),
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		aKey, _ := aMetaObj.Key.ValueAsString(aRecord.Data["id"])
		bRecord, err := dataProcessor.CreateRecord(
			bMetaObj.Name,
			map[string]interface{}{"target_object": map[string]interface{}{"_object": aMetaObj.Name, "id": aKey}},
			auth.User{},
		)
		Expect(err).To(BeNil())
		bKey, _ := bMetaObj.Key.ValueAsString(bRecord.Data["id"])

		//remove A record
		removedData, err := dataProcessor.RemoveRecord(aMetaObj.Name, aKey, auth.User{})
		Expect(err).To(BeNil())

		//check A record does not exist
		record, _ := dataProcessor.Get(aMetaObj.Name, aKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check B record does not exist
		record, err = dataProcessor.Get(bMetaObj.Name, bKey, nil, nil, 1, false)
		Expect(record).To(BeNil())

		//check removed data tree
		Expect(removedData).To(Not(BeNil()))
	})
})
