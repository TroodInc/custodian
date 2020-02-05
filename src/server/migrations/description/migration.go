package description

import (
	"encoding/json"
	"io"
	"server/data/notifications"
	"server/data/record"
	"server/errors"
	_migrations "server/migrations"
	"server/object/meta"
	"strings"
)

type MigrationDescription struct {
	Id              string                          `json:"id"`
	ApplyTo         string                          `json:"applyTo"`
	DependsOn       []string                        `json:"dependsOn"`
	Operations      []MigrationOperationDescription `json:"operations"`
	MetaDescription *meta.Meta    `json:"metaState,omitempty"`
}

func MigrationDescriptionFromRecord(record *record.Record) (*MigrationDescription){
	metaDescription, _ := MigrationMetaDescriptionFromJson(strings.NewReader(record.Data["meta_state"].(string)))
	migrationDescription := MigrationDescription{
		record.Data["migration_id"].(string),
		record.Data["object"].(string),
		[]string{record.Data["predecessor_id"].(string)},
		[]MigrationOperationDescription{},
		metaDescription.MetaDescription(),
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
	meta.Field
	PreviousName string `json:"previousName"`
}

type MigrationActionDescription struct {
	notifications.Action
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

func (mmd *MigrationMetaDescription) MetaDescription() *meta.Meta {
	fields := make([]*meta.Field, 0)
	for i := range mmd.Fields {
		fields = append(fields, mmd.Fields[i].Field.Clone())
	}

	actions := make([]*notifications.Action, 0)
	for i := range mmd.Actions {
		actions = append(actions, mmd.Actions[i].Action.Clone())
	}

	return meta.NewMeta(mmd.Name, mmd.Key, fields, actions, mmd.Cas)
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
	MetaDescription *meta.Meta `json:"object,omitempty"`
	Action          *MigrationActionDescription  `json:"action,omitempty"`
}

func NewMigrationOperationDescription(operationType string, field *MigrationFieldDescription, metaDescription *meta.Meta, action *MigrationActionDescription) *MigrationOperationDescription {
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
