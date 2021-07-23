package object_test

import (
	"custodian/server/object"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	migrations_description "custodian/server/migrations/description"
	"custodian/server/object/description"
	"custodian/server/object/migrations/managers"

	"custodian/utils"
)

var _ = Describe("Test cache", func() {
	appConfig := utils.GetConfig()
	db, _ := object.NewDbConnection(appConfig.DbConnectionUrl)
	//transaction managers
	dbTransactionManager := object.NewPgDbTransactionManager(db)

	metaDescriptionSyncer := object.NewPgMetaDescriptionSyncer(dbTransactionManager, object.NewCache())
	migrationManager := managers.NewMigrationManager(
		metaDescriptionSyncer, dbTransactionManager,
	)
	metaStore := object.NewStore(metaDescriptionSyncer, dbTransactionManager)

	flushDb := func() {

		err := dbTransactionManager.ExecStmt(managers.TRUNCATE_MIGRATION_HISTORY_TABLE)
		Expect(err).To(BeNil())
		err = metaStore.Flush()
		Expect(err).To(BeNil())
		metaDescriptionSyncer.Cache().Flush()
	}
	AfterEach(func() {
		flushDb()
	})

	createObjectFromMigration := func(metaDescription *description.MetaDescription) {

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        utils.RandomString(8),
			ApplyTo:   "",
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.CreateObjectOperation,
					MetaDescription: metaDescription,
				},
			},
		}

		_, err := migrationManager.Apply(migrationDescription, true, false)
		Expect(err).To(BeNil())
	}
	Context("Cache filling in different cases", func() {

		It("Can fill cahce on simple object create", func() {
			testObjAName := utils.RandomString(8)
			simpleMetaDescription := object.GetBaseMetaData(testObjAName)

			createObjectFromMigration(simpleMetaDescription)
			Expect(metaDescriptionSyncer.Cache().GetList()).To(HaveLen(1))
			Expect(metaDescriptionSyncer.Cache().Get(testObjAName)).NotTo(BeNil())

		})
	})
	It("Can resolve meta", func() {
		testObjAName := utils.RandomString(8) + "_a"
		simpleMetaDescription := object.GetBaseMetaData(testObjAName)

		meta, err := metaDescriptionSyncer.Cache().FactoryMeta(simpleMetaDescription)
		Expect(err).To(BeNil())

		Expect(meta.Fields).To(HaveLen(1))
		Expect(meta.Name).To(Equal(testObjAName))

		Expect(meta.Fields[0].Name).To(Equal("id"))
		Expect(meta.Fields[0].LinkMeta).To(BeNil())
		Expect(meta.Fields[0].LinkThrough).To(BeNil())
		Expect(meta.Fields[0].LinkMetaList.GetAll()).To(HaveLen(0))
		Expect(meta.Key.Meta.MetaDescription).To(Equal(meta.Fields[0].Meta.MetaDescription))
		Expect(meta.Actions).To(HaveLen(0))

	})
	It("adds cache for generic object", func() {
		testObjAName := utils.RandomString(8) + "_a"
		testObjBName := utils.RandomString(8) + "_b"

		aMetaDescription := object.GetBaseMetaData(testObjAName)

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        utils.RandomString(8),
			ApplyTo:   "",
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.CreateObjectOperation,
					MetaDescription: aMetaDescription,
				},
			},
		}

		_, err := migrationManager.Apply(migrationDescription, true, false)
		Expect(err).To(BeNil())

		bMetaDescription := description.NewMetaDescription(
			testObjBName,
			"id",
			[]description.Field{
				{
					Name: "id",
					Type: description.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:         "target_object",
					Type:         description.FieldTypeGeneric,
					LinkType:     description.LinkTypeInner,
					LinkMetaList: []string{aMetaDescription.Name},
					Optional:     false,
				},
			},
			nil,
			false,
		)

		migrationDescription = &migrations_description.MigrationDescription{
			Id:        utils.RandomString(8),
			ApplyTo:   "",
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.CreateObjectOperation,
					MetaDescription: bMetaDescription,
				},
			},
		}

		_, err = migrationManager.Apply(migrationDescription, true, false)
		Expect(err).To(BeNil())

		Expect(metaDescriptionSyncer.Cache().Get(testObjAName)).NotTo(BeNil())
		Expect(metaDescriptionSyncer.Cache().Get(testObjBName)).NotTo(BeNil())

		Expect(metaDescriptionSyncer.Cache().Get(testObjAName).Fields).To(HaveLen(2))
		Expect(metaDescriptionSyncer.Cache().Get(testObjAName).Fields[0].Name).To(Equal("id"))
		Expect(metaDescriptionSyncer.Cache().Get(testObjAName).Fields[1].Name).To(Equal(testObjBName + "_set"))

		Expect(metaDescriptionSyncer.Cache().Get(testObjBName).Fields).To(HaveLen(2))
		Expect(metaDescriptionSyncer.Cache().Get(testObjBName).Fields[0].Name).To(Equal("id"))
		Expect(metaDescriptionSyncer.Cache().Get(testObjBName).Fields[1].Name).To(Equal("target_object"))
		Expect(metaDescriptionSyncer.Cache().Get(testObjBName).Fields[1].LinkMetaList.GetAll()).To(HaveLen(1))
		Expect(metaDescriptionSyncer.Cache().Get(testObjBName).Fields[1].LinkMetaList.GetAll()[0].Name).To(Equal(testObjAName))

		Expect(metaDescriptionSyncer.Cache().Get(testObjBName).Actions).To(HaveLen(0))

	})

	It("Cache changes if object change", func() {
		testObjAName := utils.RandomString(8) + "_a"

		aMetaDescription := object.GetBaseMetaData(testObjAName)

		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "1_" + utils.RandomString(8),
			ApplyTo:   "",
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.CreateObjectOperation,
					MetaDescription: aMetaDescription,
				},
			},
		}

		_, err := migrationManager.Apply(migrationDescription, true, false)
		Expect(err).To(BeNil())

		field := description.Field{
			Name:     "title",
			Type:     description.FieldTypeString,
			Optional: false,
		}

		addFieldMigrationDescription := &migrations_description.MigrationDescription{
			Id:        "2_" + utils.RandomString(8),
			ApplyTo:   aMetaDescription.Name,
			DependsOn: []string{migrationDescription.Id},
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:  migrations_description.AddFieldOperation,
					Field: &migrations_description.MigrationFieldDescription{Field: field},
				},
			},
		}

		_, err = migrationManager.Apply(addFieldMigrationDescription, true, false)
		Expect(err).To(BeNil())
		Expect(metaDescriptionSyncer.Cache().GetList()).To(HaveLen(1))

		Expect(metaDescriptionSyncer.Cache().Get(testObjAName)).NotTo(BeNil())

		Expect(metaDescriptionSyncer.Cache().Get(testObjAName).Fields).To(HaveLen(2))
		Expect(metaDescriptionSyncer.Cache().Get(testObjAName).Fields[0].Name).To(Equal("id"))
		Expect(metaDescriptionSyncer.Cache().Get(testObjAName).Fields[1].Name).To(Equal("title"))

	})
	It("can add inner and outer links to test", func() {
		testObjAName := utils.RandomString(8) + "_a"
		aMetaDescription := object.GetBaseMetaData(testObjAName)

		testObjBName := utils.RandomString(8) + "_b"
		bMetaDescription := object.GetBaseMetaData(testObjBName)

		aMigrationDescription := &migrations_description.MigrationDescription{
			Id:        "1_" + utils.RandomString(8),
			ApplyTo:   "",
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.CreateObjectOperation,
					MetaDescription: aMetaDescription,
				},
			},
		}

		_, err := migrationManager.Apply(aMigrationDescription, true, false)
		Expect(err).To(BeNil())

		bMigrationDescription := &migrations_description.MigrationDescription{
			Id:        "2_" + utils.RandomString(8),
			ApplyTo:   "",
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.CreateObjectOperation,
					MetaDescription: bMetaDescription,
				},
			},
		}

		_, err = migrationManager.Apply(bMigrationDescription, true, false)
		Expect(err).To(BeNil())

		field := description.Field{
			Name:     testObjBName,
			Type:     description.FieldTypeObject,
			LinkType: description.LinkTypeInner,
			LinkMeta: testObjBName,
			Optional: false,
		}

		addFieldMigrationDescription := &migrations_description.MigrationDescription{
			Id:        "3_" + utils.RandomString(8),
			ApplyTo:   aMetaDescription.Name,
			DependsOn: []string{aMigrationDescription.Id},
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:  migrations_description.AddFieldOperation,
					Field: &migrations_description.MigrationFieldDescription{Field: field},
				},
			},
		}

		_, err = migrationManager.Apply(addFieldMigrationDescription, true, false)
		Expect(err).To(BeNil())

		Expect(metaDescriptionSyncer.Cache().GetList()).To(HaveLen(2))

		Expect(metaDescriptionSyncer.Cache().Get(testObjAName)).NotTo(BeNil())

		Expect(metaDescriptionSyncer.Cache().Get(testObjAName).Fields).To(HaveLen(2))
		Expect(metaDescriptionSyncer.Cache().Get(testObjAName).Fields[0].Name).To(Equal("id"))
		Expect(metaDescriptionSyncer.Cache().Get(testObjAName).Fields[1].Name).To(Equal(testObjBName))
		Expect(metaDescriptionSyncer.Cache().Get(testObjAName).Fields[1].LinkMeta).NotTo(BeNil())

		Expect(metaDescriptionSyncer.Cache().Get(testObjBName)).NotTo(BeNil())
		Expect(metaDescriptionSyncer.Cache().Get(testObjBName).Fields).To(HaveLen(2))
		Expect(metaDescriptionSyncer.Cache().Get(testObjBName).Fields[1].OuterLinkField).NotTo(BeNil())

		field = description.Field{
			Name:           testObjAName,
			Type:           description.FieldTypeArray,
			LinkType:       description.LinkTypeOuter,
			LinkMeta:       testObjAName,
			OuterLinkField: testObjBName,
			Optional:       false,
		}

		fieldMigrationDescription := &migrations_description.MigrationDescription{
			Id:        "4_" + utils.RandomString(8),
			ApplyTo:   bMetaDescription.Name,
			DependsOn: nil,
			Operations: []migrations_description.MigrationOperationDescription{
				{
					Type:  migrations_description.UpdateFieldOperation,
					Field: &migrations_description.MigrationFieldDescription{Field: field, PreviousName: field.Name},
				},
			},
		}
		_, err = migrationManager.Apply(fieldMigrationDescription, true, false)
		Expect(err).To(BeNil())
		Expect(metaDescriptionSyncer.Cache().Get(testObjBName)).NotTo(BeNil())
		Expect(metaDescriptionSyncer.Cache().Get(testObjBName).Fields).To(HaveLen(2))
		Expect(metaDescriptionSyncer.Cache().Get(testObjBName).Fields[1].OuterLinkField).NotTo(BeNil())
	})
})
