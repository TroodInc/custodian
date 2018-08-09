package meta

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"server/pg"
	"server/meta"
	"utils"
	"database/sql"
)

var _ = Describe("Inner generic field", func() {
	appConfig := utils.GetConfig()
	syncer, _ := pg.NewSyncer(appConfig.DbConnectionOptions)
	metaStore := object.NewStore(object.NewFileMetaDriver("./"), syncer)

	BeforeEach(func() {
		metaStore.Flush()
	})

	AfterEach(func() {
		metaStore.Flush()
	})

	It("can create object with inner generic field", func() {
		By("having two objects: A and B")
		aMetaDescription := object.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
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

		bMetaDescription := object.MetaDescription{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("and object C, containing generic inner field")

		cMetaDescription := object.MetaDescription{
			Name: "c",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target",
					Type:         object.FieldTypeGeneric,
					LinkType:     object.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name, bMetaObj.Name},
					Optional:     false,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		//check database columns
		db, err := sql.Open("postgres", appConfig.DbConnectionOptions)
		tx, err := db.Begin()
		Expect(err).To(BeNil())

		tableName := pg.GetTableName(metaObj)

		reverser, err := pg.NewReverser(tx, tableName)
		columns := make([]pg.Column, 0)
		pk := ""
		reverser.Columns(&columns, &pk)
		Expect(columns).To(HaveLen(3))
		// check meta fields
		cMeta, _, err := metaStore.Get(cMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(cMeta.Fields).To(HaveLen(2))
		Expect(cMeta.Fields[1].LinkMetaList.GetAll()).To(HaveLen(2))
	})

	It("Validates linked metas", func() {
		By("having an object A, referencing non-existing object B")

		cMetaDescription := object.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target",
					Type:         object.FieldTypeGeneric,
					LinkType:     object.LinkTypeInner,
					LinkMetaList: []string{"b"},
					Optional:     false,
				},
			},
		}
		By("Meta should not be created")
		_, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(Not(BeNil()))
	})

	It("can remove generic field from object", func() {
		By("having object A with generic field")
		metaDescription := object.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target",
					Type:         object.FieldTypeGeneric,
					LinkType:     object.LinkTypeInner,
					LinkMetaList: []string{},
					Optional:     false,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())
		By("when generic field is removed from object and object has been updated")

		metaDescription = object.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		metaObj, err = metaStore.NewMeta(&metaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(metaObj.Name, metaObj, true, true)
		Expect(err).To(BeNil())

		//check database columns
		db, err := sql.Open("postgres", appConfig.DbConnectionOptions)
		tx, err := db.Begin()
		Expect(err).To(BeNil())

		tableName := pg.GetTableName(metaObj)

		reverser, err := pg.NewReverser(tx, tableName)
		columns := make([]pg.Column, 0)
		pk := ""
		reverser.Columns(&columns, &pk)
		Expect(columns).To(HaveLen(1))
		Expect(columns[0].Name).To(Equal("id"))
		// check meta fields
		cMeta, _, err := metaStore.Get(metaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(cMeta.Fields).To(HaveLen(1))
		Expect(cMeta.Fields[0].Name).To(Equal("id"))

	})

	It("does not leave orphan links in LinkMetaList on object removal", func() {
		By("having two objects A and B reference by generic field of object C")
		aMetaDescription := object.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
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

		bMetaDescription := object.MetaDescription{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		cMetaDescription := object.MetaDescription{
			Name: "c",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target",
					Type:         object.FieldTypeGeneric,
					LinkType:     object.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name, bMetaObj.Name},
					Optional:     false,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		By("since object A is deleted, it should be removed from LinkMetaList")

		_, err = metaStore.Remove(aMetaObj.Name, false, true)
		Expect(err).To(BeNil())

		cMetaObj, _, err := metaStore.Get(cMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(cMetaObj.Fields[1].LinkMetaList.GetAll()).To(HaveLen(1))
	})

	It("can create object with inner generic field", func() {
		By("having object A")
		aMetaDescription := object.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
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

		By("and object C, containing generic inner field")

		cMetaDescription := object.MetaDescription{
			Name: "c",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target",
					Type:         object.FieldTypeGeneric,
					LinkType:     object.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name},
					Optional:     false,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		// check meta fields
		aMeta, _, err := metaStore.Get(aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))
		Expect(aMeta.Fields[1].Name).To(Equal("c__set"))
		Expect(aMeta.Fields[1].LinkType).To(Equal(object.LinkTypeOuter))
		Expect(aMeta.Fields[1].Type).To(Equal(object.FieldTypeGeneric))
	})

	It("can create object with inner generic field", func() {
		By("having two objects: A and B")
		aMetaDescription := object.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
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

		bMetaDescription := object.MetaDescription{
			Name: "b",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
			},
		}
		bMetaObj, err := metaStore.NewMeta(&bMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(bMetaObj)
		Expect(err).To(BeNil())

		By("and object C, containing generic inner field")

		cMetaDescription := object.MetaDescription{
			Name: "c",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target",
					Type:         object.FieldTypeGeneric,
					LinkType:     object.LinkTypeInner,
					LinkMetaList: []string{aMetaObj.Name, bMetaObj.Name},
					Optional:     false,
				},
			},
		}
		metaObj, err := metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		err = metaStore.Create(metaObj)
		Expect(err).To(BeNil())

		// check A meta fields
		aMeta, _, err := metaStore.Get(cMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(2))

		By("removing object A from object`s C LinkMetaList")
		cMetaDescription = object.MetaDescription{
			Name: "c",
			Key:  "id",
			Cas:  false,
			Fields: []object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target",
					Type:         object.FieldTypeGeneric,
					LinkType:     object.LinkTypeInner,
					LinkMetaList: []string{bMetaObj.Name},
					Optional:     false,
				},
			},
		}
		metaObj, err = metaStore.NewMeta(&cMetaDescription)
		Expect(err).To(BeNil())
		_, err = metaStore.Update(metaObj.Name, metaObj, true, true)
		Expect(err).To(BeNil())

		//c__set field should be removed from object A
		// check A meta fields
		aMeta, _, err = metaStore.Get(aMetaDescription.Name, true)
		Expect(err).To(BeNil())
		Expect(aMeta.Fields).To(HaveLen(1))
	})
})
