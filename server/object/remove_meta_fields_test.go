package object

import (
	"custodian/server/object/description"

	"custodian/utils"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests inner and outer objects update and removal", func() {
	appConfig := utils.GetConfig()
	db, _ := NewDbConnection(appConfig.DbConnectionUrl)
	//transaction managers
	dbTransactionManager := NewPgDbTransactionManager(db)

	metaDescriptionSyncer := NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := NewStore(metaDescriptionSyncer, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	testObjAName := fmt.Sprintf("%s_a", utils.RandomString(8))
	testObjBName := fmt.Sprintf("%s_b", utils.RandomString(8))
	testObjCName := fmt.Sprintf("%s_c", utils.RandomString(8))

	havingObjectA := func() *Meta {
		bMetaDescription := description.MetaDescription{
			Name: testObjAName,
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
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&bMetaDescription)
		(&description.NormalizationService{}).Normalize(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	havingObjectB := func() *Meta {
		metaDescription := description.MetaDescription{
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
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	havingObjectC := func() *Meta {
		metaDescription := description.MetaDescription{
			Name: testObjCName,
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
					Name:     testObjBName,
					Type:     description.FieldTypeObject,
					LinkType: description.LinkTypeInner,
					LinkMeta: testObjBName,
					Optional: true,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	checkNoOrphansLeft := func(name string) {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())
		tx := globalTransaction.Transaction()

		tableName := GetTableName(name)

		reverser, err := NewReverser(tx, tableName)
		columns := make([]Column, 0)
		pk := ""
		reverser.Columns(&columns, &pk)
		dbTransactionManager.CommitTransaction(globalTransaction)
		Expect(columns).To(HaveLen(1))
		Expect(columns[0].Name).To(Equal("id"))
		// check meta fields
		objMeta, _, err := metaStore.Get(name, false)
		Expect(err).To(BeNil())
		Expect(objMeta.Fields).To(HaveLen(1))
		Expect(objMeta.Fields[0].Name).To(Equal("id"))
	}

	It("Does not leave orphan on object removal if both inner and outer related objects presence", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()
		metaObjC := havingObjectC()

		_, err := metaStore.Remove(metaObjB.Name, true)
		Expect(err).To(BeNil())

		checkNoOrphansLeft(metaObjA.Name)
		checkNoOrphansLeft(metaObjC.Name)
	})

	It("Removes outer _set field", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()

		_, err := metaStore.Remove(metaObjB.Name, false)
		Expect(err).To(BeNil())
		checkNoOrphansLeft(metaObjA.Name)

	})

	It("Removes inner link if outer object is removed", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()

		_, err := metaStore.Remove(metaObjA.Name, true)
		Expect(err).To(BeNil())

		checkNoOrphansLeft(metaObjB.Name)
	})

	It("Does not leave orphan outer link on object update if inner link removed", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()

		updatedbMetaDescription := description.MetaDescription{
			Name: testObjBName,
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
				},
			},
		}
		updatedBMetaObj, err := metaStore.NewMeta(&updatedbMetaDescription)

		_, err = metaStore.Update(metaObjB.Name, updatedBMetaObj, false, true)
		Expect(err).To(BeNil())
		checkNoOrphansLeft(metaObjA.Name)

	})

	It("Does not remove inner link on update if outer link is removed", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()

		updatedbMetaDescription := description.MetaDescription{
			Name: metaObjA.Name,
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
				},
			},
		}
		updatedAMetaObj, err := metaStore.NewMeta(&updatedbMetaDescription)

		_, err = metaStore.Update(metaObjA.Name, updatedAMetaObj, false, true)
		Expect(err).To(BeNil())

		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())
		tx := globalTransaction.Transaction()

		tableName := GetTableName(metaObjB.Name)

		reverser, err := NewReverser(tx, tableName)
		columns := make([]Column, 0)
		pk := ""
		reverser.Columns(&columns, &pk)
		dbTransactionManager.CommitTransaction(globalTransaction)
		Expect(columns).To(HaveLen(2))
		Expect(columns[0].Name).To(Equal("id"))
		Expect(columns[1].Name).To(Equal(testObjAName))
		// check meta fields
		cMeta, _, err := metaStore.Get(metaObjB.Name, false)
		Expect(err).To(BeNil())
		Expect(cMeta.Fields).To(HaveLen(2))
		Expect(cMeta.Fields[0].Name).To(Equal("id"))
		Expect(cMeta.Fields[1].Name).To(Equal(testObjAName))

	})

})

var _ = Describe("Tests  generic inner and generic outer objects update and removal", func() {
	appConfig := utils.GetConfig()
	db, _ := NewDbConnection(appConfig.DbConnectionUrl)
	//transaction managers
	dbTransactionManager := NewPgDbTransactionManager(db)

	metaDescriptionSyncer := NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := NewStore(metaDescriptionSyncer, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})
	testObjAName := fmt.Sprintf("%s_a", utils.RandomString(8))
	testObjBName := fmt.Sprintf("%s_b", utils.RandomString(8))
	testObjCName := fmt.Sprintf("%s_c", utils.RandomString(8))
	testObjDName := fmt.Sprintf("%s_d", utils.RandomString(8))
	testObjEName := fmt.Sprintf("%s_e", utils.RandomString(8))
	testObjDSetName := fmt.Sprintf("%s_set", testObjDName)

	havingObjectA := func() *Meta {
		bMetaDescription := description.MetaDescription{
			Name: testObjAName,
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
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&bMetaDescription)
		(&description.NormalizationService{}).Normalize(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	havingObjectB := func() *Meta {
		metaDescription := description.MetaDescription{
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjAName},
					Optional:     true,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	havingObjectE := func() *Meta {
		bMetaDescription := description.MetaDescription{
			Name: testObjEName,
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
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&bMetaDescription)
		(&description.NormalizationService{}).Normalize(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	havingObjectC := func() *Meta {
		metaDescription := description.MetaDescription{
			Name: testObjCName,
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjBName},
					Optional:     true,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	havingObjectD := func() *Meta {
		metaDescription := description.MetaDescription{
			Name: testObjDName,
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
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjEName, testObjAName},
					Optional:     true,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	checkNoOrphansLeft := func(name string) {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())
		tx := globalTransaction.Transaction()

		tableName := GetTableName(name)

		reverser, err := NewReverser(tx, tableName)
		columns := make([]Column, 0)
		pk := ""
		reverser.Columns(&columns, &pk)
		dbTransactionManager.CommitTransaction(globalTransaction)
		Expect(columns).To(HaveLen(1))
		Expect(columns[0].Name).To(Equal("id"))
		// check meta fields
		objMeta, _, err := metaStore.Get(name, true)
		Expect(err).To(BeNil())
		Expect(objMeta.Fields).To(HaveLen(1))
		Expect(objMeta.Fields[0].Name).To(Equal("id"))
	}

	It("Removes outer _set field generic", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()

		_, err := metaStore.Remove(metaObjB.Name, true)
		Expect(err).To(BeNil())

		checkNoOrphansLeft(metaObjA.Name)

	})

	It("Flush LinkMetaList if related outer generic object is removed", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()
		// havingObjectB()
		_, err := metaStore.Remove(metaObjA.Name, false)
		Expect(err).To(BeNil())

		checkNoOrphansLeft(metaObjB.Name)

	})

	It("Does not leave orphan on object update if inner object is updated", func() {
		metaObjA := havingObjectA()
		metaObjE := havingObjectE()
		metaObjD := havingObjectD()

		By("Update inner field, remove inner link.")
		updatedbMetaDescriptionD := description.MetaDescription{
			Name: testObjDName,
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
				},
				{
					Name:         "target",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{testObjAName},
					Optional:     true,
				},
			},
		}
		updatedBMetaObj, err := metaStore.NewMeta(&updatedbMetaDescriptionD)

		_, err = metaStore.Update(metaObjD.Name, updatedBMetaObj, false, true)
		Expect(err).To(BeNil())

		By("Check only inner link to eObj is removed")
		dMeta, _, err := metaStore.Get(metaObjD.Name, true)
		Expect(err).To(BeNil())
		Expect(dMeta.Fields).To(HaveLen(2))
		Expect(dMeta.Fields[0].Name).To(Equal("id"))
		Expect(dMeta.Fields[1].Name).To(Equal("target"))
		Expect(dMeta.Fields[1].LinkMetaList.GetAll()).To(HaveLen(1))
		Expect(dMeta.Fields[1].LinkMetaList.GetAll()[0].Name).To(Equal(metaObjA.Name))

		By("Check eObj has no orphans")
		checkNoOrphansLeft(metaObjE.Name)

		By("aObj has related outer field")
		aMeta, _, err := metaStore.Get(metaObjA.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.Fields[0].Name).To(Equal("id"))
		Expect(aMeta.Fields[1].Name).To(Equal(testObjDSetName))

	})

	It("Does not leave orphan on object removal if both outer and inner presence", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()
		metaObjC := havingObjectC()

		_, err := metaStore.Remove(metaObjB.Name, true)
		Expect(err).To(BeNil())
		By("outer object removed")
		checkNoOrphansLeft(metaObjA.Name)

		By("Flush LinkMetaList in related inner object")
		checkNoOrphansLeft(metaObjC.Name)

	})

	It("Flush LinkMetaList if related inner generic object is removed on object update", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()

		updatedbMetaDescription := description.MetaDescription{
			Name: testObjBName,
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
				},
			},
		}
		updatedBMetaObj, err := metaStore.NewMeta(&updatedbMetaDescription)

		_, err = metaStore.Update(metaObjB.Name, updatedBMetaObj, false, true)
		Expect(err).To(BeNil())
		checkNoOrphansLeft(metaObjA.Name)

	})

	It("Should not flush LinkMetaList if related outer generic object is removed on object update", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()

		updatedAMetaDescription := description.MetaDescription{
			Name: testObjAName,
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
				},
			},
		}
		updatedAMetaObj, err := metaStore.NewMeta(&updatedAMetaDescription)

		_, err = metaStore.Update(metaObjA.Name, updatedAMetaObj, false, true)
		Expect(err).To(BeNil())
		bMeta, _, err := metaStore.Get(metaObjB.Name, true)
		Expect(err).To(BeNil())
		Expect(bMeta.Fields).To(HaveLen(2))
		Expect(bMeta.Fields[0].Name).To(Equal("id"))
		Expect(bMeta.Fields[1].Name).To(Equal("target"))
		Expect(bMeta.Fields[1].LinkMetaList.GetAll()).To(HaveLen(1))
		Expect(bMeta.Fields[1].LinkMetaList.GetAll()[0].Name).To(Equal(metaObjA.Name))

	})

})
var _ = Describe("Remove m2m fields", func() {
	appConfig := utils.GetConfig()
	db, _ := NewDbConnection(appConfig.DbConnectionUrl)
	//transaction managers
	dbTransactionManager := NewPgDbTransactionManager(db)

	metaDescriptionSyncer := NewPgMetaDescriptionSyncer(dbTransactionManager)
	metaStore := NewStore(metaDescriptionSyncer, dbTransactionManager)

	AfterEach(func() {
		err := metaStore.Flush()
		Expect(err).To(BeNil())
	})

	testObjAName := fmt.Sprintf("%s_a", utils.RandomString(8))
	testObjBName := fmt.Sprintf("%s_b", utils.RandomString(8))

	havingObjectA := func() *Meta {
		bMetaDescription := description.MetaDescription{
			Name: testObjAName,
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
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&bMetaDescription)
		(&description.NormalizationService{}).Normalize(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	havingObjectB := func() *Meta {
		metaDescription := description.MetaDescription{
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
					Name:     testObjBName,
					Type:     description.FieldTypeObjects,
					LinkMeta: testObjAName,
					LinkType: description.LinkTypeInner,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		return metaObj
	}

	checkNoOrphansLeft := func(name string) {
		globalTransaction, err := dbTransactionManager.BeginTransaction()
		Expect(err).To(BeNil())
		tx := globalTransaction.Transaction()

		tableName := GetTableName(name)

		reverser, err := NewReverser(tx, tableName)
		columns := make([]Column, 0)
		pk := ""
		reverser.Columns(&columns, &pk)
		dbTransactionManager.CommitTransaction(globalTransaction)
		Expect(columns).To(HaveLen(1))
		Expect(columns[0].Name).To(Equal("id"))
		// check meta fields
		aMeta, _, err := metaStore.Get(name, false)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(1))
		Expect(aMeta.Fields[0].Name).To(Equal("id"))
	}

	It("Does not leave orphan in related object if outer m2m obj deleted", func() {

		metaObjA := havingObjectA()
		metaObjB := havingObjectB()

		_, err := metaStore.Remove(metaObjA.Name, false)
		Expect(err).To(BeNil())

		checkNoOrphansLeft(metaObjB.Name)
	})

	It("Does not leave orphan in related object if inner m2m obj deleted", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()

		_, err := metaStore.Remove(metaObjB.Name, false)
		Expect(err).To(BeNil())

		checkNoOrphansLeft(metaObjA.Name)

	})

	It("Does not leave orphan in outer related object if inner m2m field is deleted on update", func() {
		metaObjA := havingObjectA()
		metaObjB := havingObjectB()

		updatedbMetaDescription := description.MetaDescription{
			Name: testObjBName,
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
				},
			},
		}
		updatedBMetaObj, err := metaStore.NewMeta(&updatedbMetaDescription)

		_, err = metaStore.Update(metaObjB.Name, updatedBMetaObj, false, true)
		Expect(err).To(BeNil())
		checkNoOrphansLeft(metaObjA.Name)
	})
})
