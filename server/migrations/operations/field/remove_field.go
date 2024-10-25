package field

import (
	"custodian/server/errors"
	"custodian/server/migrations"
	"custodian/server/object"
	"custodian/server/object/description"
	"fmt"
)

type RemoveFieldOperation struct {
	Field *description.Field
}

func (o *RemoveFieldOperation) SyncMetaDescription(metaDescription *description.MetaDescription, metaDescriptionSyncer object.MetaDescriptionSyncer) (*description.MetaDescription, error) {
	updatedMetaDescription := metaDescription.Clone()
	if err := o.validate(updatedMetaDescription); err != nil {
		return nil, err
	}

	updatedMetaDescription.Fields = make([]description.Field, 0)

	//remove field from meta description
	for i, currentField := range metaDescription.Fields {
		if currentField.Name != o.Field.Name {
			updatedMetaDescription.Fields = append(updatedMetaDescription.Fields, metaDescription.Fields[i])
		}
	}
	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Update(updatedMetaDescription.Name, *updatedMetaDescription); err != nil {
		return nil, err
	} else {
		return updatedMetaDescription, nil
	}
}

func (o *RemoveFieldOperation) validate(metaDescription *description.MetaDescription) error {
	if metaDescription.FindField(o.Field.Name) == nil {
		return errors.NewValidationError(
			migrations.MigrationErrorInvalidDescription,
			fmt.Sprintf("Object %s has no field named %s", metaDescription.Name, o.Field.Name),
			nil,
		)
	}
	return nil
}

func NewRemoveFieldOperation(field *description.Field) *RemoveFieldOperation {
	return &RemoveFieldOperation{Field: field}
}
