package description

import (
	"server/object/description"
)

type MigrationDescription struct {
	Id         string                          `json:"id"`
	ApplyTo    string                          `json:"applyTo"`
	DependsOn  []string                        `json:"dependsOn"`
	Operations []MigrationOperationDescription `json:"operations"`
}

type MigrationFieldDescription struct {
	description.Field
	PreviousName string `json:"previousName"`
}


type MigrationOperationDescription struct {
	Type            string                      `json:"type"`
	Field           MigrationFieldDescription   `json:"field,omitempty"`
	MetaDescription description.MetaDescription `json:"object,omitempty"`
}

const (
	AddFieldOperation    = "addField"
	RemoveFieldOperation = "removeField"
	UpdateFieldOperation = "updateField"

	CreateObjectOperation = "createObject"
	DeleteObjectOperation = "deleteObject"
	RenameObjectOperation = "renameObject"
)
