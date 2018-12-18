package description

import (
	"server/object/description"
	"encoding/json"
	"io"
)

type MigrationDescription struct {
	Id         string                          `json:"id"`
	ApplyTo    string                          `json:"applyTo"`
	DependsOn  []string                        `json:"dependsOn"`
	Operations []MigrationOperationDescription `json:"operations"`
}

func (md *MigrationDescription) Marshal() ([]byte, error) {
	return json.Marshal(md)
}

func (md *MigrationDescription) Unmarshal(inputReader io.ReadCloser) (*MigrationDescription, error) {
	if e := json.NewDecoder(inputReader).Decode(md); e != nil {
		return nil, NewMigrationUnmarshallingError(e.Error())
	}
	return md, nil
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
