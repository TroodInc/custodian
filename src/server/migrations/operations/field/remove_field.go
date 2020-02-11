package field

import (
	"fmt"
	"server/errors"
	"server/migrations"
	"server/object"
	"server/transactions"
)

type RemoveFieldOperation struct {
	Field *object.Field
}

func (o *RemoveFieldOperation) SyncMetaDescription(metaDescription *object.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer object.MetaDescriptionSyncer) (*object.Meta, error) {
	updatedMetaDescription := metaDescription.Clone()
	if err := o.validate(updatedMetaDescription); err != nil {
		return nil, err
	}

	updatedMetaDescription.Fields = make([]*object.Field, 0)

	//remove field from meta description
	for i, currentField := range metaDescription.Fields {
		if currentField.Name != o.Field.Name {
			updatedMetaDescription.Fields = append(updatedMetaDescription.Fields, metaDescription.Fields[i])
		}
	}
	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Update(updatedMetaDescription.Name, updatedMetaDescription.ForExport()); err != nil {
		return nil, err
	} else {
		return updatedMetaDescription, nil
	}
}

func (o *RemoveFieldOperation) validate(metaDescription *object.Meta) error {
	if metaDescription.FindField(o.Field.Name) == nil {
		return errors.NewValidationError(
			migrations.MigrationErrorInvalidDescription,
			fmt.Sprintf("Object %s has no field named %s", metaDescription.Name, o.Field.Name),
			nil,
		)
	}
	return nil
}

func NewRemoveFieldOperation(field *object.Field) *RemoveFieldOperation {
	return &RemoveFieldOperation{Field: field}
}
