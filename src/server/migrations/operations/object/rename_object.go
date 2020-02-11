package object

import (
	"fmt"
	"server/errors"
	"server/migrations"
	"server/object"
	"server/transactions"
)

type RenameObjectOperation struct {
	MetaDescription *object.Meta
}

func (o *RenameObjectOperation) SyncMetaDescription(metaDescriptionToApply *object.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer object.MetaDescriptionSyncer) (*object.Meta, error) {
	if err := o.validate(metaDescriptionToApply, metaDescriptionSyncer); err != nil {
		return nil, err
	}

	//remove old description
	metaDescriptionSyncer.Remove(metaDescriptionToApply.Name)
	//create new one
	metaDescriptionSyncer.Create(transaction, o.MetaDescription.Name, o.MetaDescription.ForExport())

	return o.MetaDescription, nil
}

func (o *RenameObjectOperation) validate(metaObj *object.Meta, metaDescriptionSyncer object.MetaDescriptionSyncer) error {
	metaDescription, _, _ := metaDescriptionSyncer.Get(o.MetaDescription.Name)
	if metaDescription != nil {
		return errors.NewValidationError(
			migrations.MigrationErrorInvalidDescription,
			fmt.Sprintf("failed to rename object '%s' to '%s': object named '%s' already exists", metaObj.Name, o.MetaDescription.Name, o.MetaDescription.Name),
			nil,
		)
	}
	return nil
}
