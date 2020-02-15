package field

import (
	"fmt"
	"server/errors"
	"server/migrations"
	"server/object"
	"server/object/meta"
	"server/transactions"
)

type RemoveFieldOperation struct {
	Field *meta.Field
}

func (o *RemoveFieldOperation) SyncMetaDescription(metaDescription *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer *object.Store) (*meta.Meta, error) {
	updatedMetaDescription := metaDescription.Clone()
	if err := o.validate(updatedMetaDescription); err != nil {
		return nil, err
	}

	updatedMetaDescription.Fields = make(map[string]*meta.Field, 0)

	//remove field from meta description
	for _, currentField := range metaDescription.Fields {
		if currentField.Name != o.Field.Name {
			updatedMetaDescription.AddField(currentField)
		}
	}
	//sync its MetaDescription
	if meta := metaDescriptionSyncer.Update(updatedMetaDescription); meta == nil {
		return nil, errors.NewValidationError("", "Cant remove field from migration", o.Field)
	} else {
		return updatedMetaDescription, nil
	}
}

func (o *RemoveFieldOperation) validate(metaDescription *meta.Meta) error {
	if metaDescription.FindField(o.Field.Name) == nil {
		return errors.NewValidationError(
			migrations.MigrationErrorInvalidDescription,
			fmt.Sprintf("Object %s has no field named %s", metaDescription.Name, o.Field.Name),
			nil,
		)
	}
	return nil
}

func NewRemoveFieldOperation(field *meta.Field) *RemoveFieldOperation {
	return &RemoveFieldOperation{Field: field}
}
