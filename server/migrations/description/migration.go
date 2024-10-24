package description

import (
	"custodian/server/errors"
	_migrations "custodian/server/migrations"
	"custodian/server/object"
	"custodian/server/object/description"
	"encoding/json"
	"io"
	"strings"
)

type MigrationDescription struct {
	Id              string                          `json:"id"`
	ApplyTo         string                          `json:"applyTo"`
	DependsOn       []string                        `json:"dependsOn"`
	Operations      []MigrationOperationDescription `json:"operations"`
	MetaDescription *description.MetaDescription    `json:"metaState,omitempty"`
	Description     string                          `json:"description"`
}

func MigrationDescriptionFromRecord(record *object.Record) (*MigrationDescription){
	metaDescription, _ := MigrationMetaDescriptionFromJson(strings.NewReader(record.Data["meta_state"].(string)))
	migrationDescription := MigrationDescription{
		record.Data["id"].(string),
		record.Data["applyTo"].(string),
		[]string{record.Data["dependsOn"].(string)},
		[]MigrationOperationDescription{},
		metaDescription.MetaDescription(),
		record.Data["description"].(string),
	}

	json.Unmarshal([]byte(record.Data["operations"].(string)), &migrationDescription.Operations)

	return &migrationDescription
}

func MigrationDescriptionFromJson(inputReader io.Reader) (*MigrationDescription, error){
	md := MigrationDescription{}
	if e := json.NewDecoder(inputReader).Decode(&md); e != nil {
		return nil, errors.NewValidationError("cant_unmarshal_migration", e.Error(), nil)
	}
	return &md, nil
}

func BulkMigrationDescriptionFromJson(b []byte) ([]*MigrationDescription, error) {
	var md []*MigrationDescription
	if e := json.Unmarshal(b, &md); e != nil {
		return nil, errors.NewValidationError("cant_unmarshal_migration", e.Error(), nil)
	}
	return md, nil
}

func (md *MigrationDescription) Marshal() ([]byte, error) {
	return json.Marshal(md)
}

//Returns meta`s name which this migration is intended for
//TODO: implement MigrationDescription`s validation in a separate step
func (md *MigrationDescription) MetaName() (string, error) {
	if md.ApplyTo != "" {
		return md.ApplyTo, nil
	} else {
		if md.Operations[0].Type != CreateObjectOperation {
			return "", errors.NewValidationError(_migrations.MigrationErrorInvalidDescription, "Migration has neither ApplyTo defined nor createObject operation", nil)
		}
		return md.Operations[0].MetaDescription.Name, nil
	}
}

type MigrationFieldDescription struct {
	description.Field
	PreviousName string `json:"previousName"`
}

type MigrationActionDescription struct {
	description.Action
	PreviousName string `json:"previousName"`
}

type MigrationMetaDescription struct {
	Name         string                       `json:"name"`
	PreviousName string                       `json:"previousName"`
	Key          string                       `json:"key"`
	Fields       []MigrationFieldDescription  `json:"fields"`
	Actions      []MigrationActionDescription `json:"actions,omitempty"`
	Cas          bool                         `json:"cas"`
}

func MigrationMetaDescriptionFromJson(inputReader io.Reader)(*MigrationMetaDescription, error)  {
	mmd := MigrationMetaDescription{}
	if e := json.NewDecoder(inputReader).Decode(&mmd); e != nil {
		return nil, errors.NewValidationError("cant_unmarshal_migration", e.Error(), nil)
	}
	return &mmd, nil
}

func (mmd *MigrationMetaDescription) MetaDescription() *description.MetaDescription {
	fields := make([]description.Field, 0)
	for i := range mmd.Fields {
		fields = append(fields, *mmd.Fields[i].Field.Clone())
	}

	actions := make([]description.Action, 0)
	for i := range mmd.Actions {
		actions = append(actions, *mmd.Actions[i].Action.Clone())
	}

	return description.NewMetaDescription(mmd.Name, mmd.Key, fields, actions, mmd.Cas)
}

func (mmd *MigrationMetaDescription) FindFieldWithPreviousName(fieldName string) *MigrationFieldDescription {
	for i := range mmd.Fields {
		if mmd.Fields[i].PreviousName == fieldName {
			return &mmd.Fields[i]
		}
	}
	return nil
}

func (mmd *MigrationMetaDescription) FindActionWithPreviousName(actionName string) *MigrationActionDescription {
	for i := range mmd.Actions {
		if mmd.Actions[i].PreviousName == actionName {
			return &mmd.Actions[i]
		}
	}
	return nil
}

type MigrationOperationDescription struct {
	Type            string                       `json:"type"`
	Field           *MigrationFieldDescription   `json:"field,omitempty"`
	MetaDescription *description.MetaDescription `json:"object,omitempty"`
	Action          *MigrationActionDescription  `json:"action,omitempty"`
}

func NewMigrationOperationDescription(operationType string, field *MigrationFieldDescription, metaDescription *description.MetaDescription, action *MigrationActionDescription) *MigrationOperationDescription {
	return &MigrationOperationDescription{Type: operationType, Field: field, MetaDescription: metaDescription, Action: action}
}

const (
	AddFieldOperation    = "addField"
	RemoveFieldOperation = "removeField"
	UpdateFieldOperation = "updateField"

	CreateObjectOperation = "createObject"
	DeleteObjectOperation = "deleteObject"
	RenameObjectOperation = "renameObject"

	AddActionOperation    = "addAction"
	UpdateActionOperation = "updateAction"
	RemoveActionOperation = "removeAction"
)
