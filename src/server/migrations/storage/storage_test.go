package storage

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	migrations_description "server/migrations/description"
	"server/object"

	"utils"
)

var _ = Describe("Migration Storage", func() {
	appConfig := utils.GetConfig()
	migrationStorage := NewMigrationStorage(appConfig.MigrationStoragePath)

	BeforeEach(func() { migrationStorage.Flush() })
	AfterEach(func() { migrationStorage.Flush() })

	It("stores MigrationMetaDescription to file", func() {
		metaDescription := &object.Meta{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []*object.Field{
				{
					Name: "id",
					Type: object.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "date",
					Type:     object.FieldTypeDate,
					Optional: false,
				},
			},
		}
		migrationDescription := &migrations_description.MigrationDescription{
			Id:        "some-unique-id",
			ApplyTo:   "a",
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
