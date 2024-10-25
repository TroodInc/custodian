package storage

import (
	"custodian/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	migrations_description "custodian/server/migrations/description"
	"custodian/server/object/description"
)

var _ = Describe("Migration Storage", func() {
	appConfig := utils.GetConfig()
	migrationStorage := NewMigrationStorage(appConfig.MigrationStoragePath)

	BeforeEach(func() { migrationStorage.Flush() })
	AfterEach(func() { migrationStorage.Flush() })
	testObjAName := utils.RandomString(8)

	It("stores MigrationMetaDescription to file", func() {
		metaDescription := &description.MetaDescription{
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
				},
				{
					Name:     "date",
					Type:     description.FieldTypeDate,
					Optional: false,
				},
			},
		}
		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   testObjAName,
			DependsOn: nil,
			Operations: [] migrations_description.MigrationOperationDescription{
				{
					Type:            migrations_description.RenameObjectOperation,
					MetaDescription: metaDescription,
				},
			},
		}
		migrationFileName, err := migrationStorage.Store(migrationDescription)
		Expect(err).To(BeNil())
		Expect(migrationFileName).To(ContainSubstring(migrationDescription.Id))
	})
})
