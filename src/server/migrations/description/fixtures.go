package description

import (
	"fmt"
	"server/object/description"
	"time"
)

func GetObjectCreationMigration(idOrRandom string, applyTo string, depends []string, metaDescription *description.MetaDescription) *MigrationDescription {
	if idOrRandom == "random" {
		idOrRandom = fmt.Sprintf("%x", time.Now().Unix())
	}
	return &MigrationDescription{
		Id:        idOrRandom,
		ApplyTo:   applyTo,
		DependsOn: depends,
		Operations: [] MigrationOperationDescription{
			{
				Type:            CreateObjectOperation,
				MetaDescription: metaDescription,
			},
		},
	}
}

func GetFieldCreationMigration(idOrRandom string, applyTo string, depends []string, field description.Field) *MigrationDescription {
	if idOrRandom == "random" {
		idOrRandom = fmt.Sprintf("%x", time.Now().Unix())
	}
	return &MigrationDescription{
		Id:        idOrRandom,
		ApplyTo:   applyTo,
		DependsOn: depends,
		Operations: [] MigrationOperationDescription{
			{
				Type:  AddFieldOperation,
				Field: &MigrationFieldDescription{Field: field},
			},
		},
	}
}
