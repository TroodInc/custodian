package storage

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	migrations_description "server/migrations/description"

	"server/object/meta"
	"utils"
)

var _ = Describe("Migration Storage", func() {
	appConfig := utils.GetConfig()
	migrationStorage := NewMigrationStorage(appConfig.MigrationStoragePath)

	BeforeEach(func() { migrationStorage.Flush() })
	AfterEach(func() { migrationStorage.Flush() })

	It("stores MigrationMetaDescription to file", func() {
		metaDescription := &description.MetaDescription{
			Name: "a",
			Key:  "id",
			Cas:  false,
			Fields: []meta.Field{
				{
					Name: "id",
					Type: meta.FieldTypeNumber,
					Def: map[string]interface{}{
						"func": "nextval",
					},
				},
				{
					Name:     "date",
					Type:     meta.FieldTypeDate,
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
