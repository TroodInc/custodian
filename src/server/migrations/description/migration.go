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

type MigrationMetaDescription struct {
	Name    string                      `json:"name"`
	Key     string                      `json:"key"`
	Fields  []MigrationFieldDescription `json:"fields"`
	Actions []description.Action        `json:"actions,omitempty"`
	Cas     bool                        `json:"cas"`
}

func (mmd *MigrationMetaDescription) MetaDescription() *description.MetaDescription {
	fields := make([]description.Field, 0)
	for i := range mmd.Fields {
		fields = append(fields, *mmd.Fields[i].Field.Clone())
	}
	return description.NewMetaDescription(mmd.Name, mmd.Key, fields, mmd.Actions, mmd.Cas)
}

func (mmd *MigrationMetaDescription) FindFieldWithPreviousName(fieldName string) *MigrationFieldDescription {
	for i := range mmd.Fields {
		if mmd.Fields[i].PreviousName == fieldName {
			return &mmd.Fields[i]
		}
	}
	return nil
}

type MigrationOperationDescription struct {
	Type            string                      `json:"type"`
	Field           MigrationFieldDescription   `json:"field,omitempty"`
	MetaDescription description.MetaDescription `json:"object,omitempty"`
}

func NewMigrationOperationDescription(operationType string, field *MigrationFieldDescription, metaDescription *description.MetaDescription) *MigrationOperationDescription {
	var fieldValue MigrationFieldDescription
	if field != nil {
		fieldValue = *field
	}

	var metaDescriptionValue description.MetaDescription
	if metaDescription != nil {
		metaDescriptionValue = *metaDescription
	}

	return &MigrationOperationDescription{Type: operationType, Field: fieldValue, MetaDescription: metaDescriptionValue}
}

const (
	AddFieldOperation    = "addField"
	RemoveFieldOperation = "removeField"
	UpdateFieldOperation = "updateField"

	CreateObjectOperation = "createObject"
	DeleteObjectOperation = "deleteObject"
	RenameObjectOperation = "renameObject"
)
