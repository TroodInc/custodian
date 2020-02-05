package object

import (
	"fmt"
	"server/errors"
	"server/migrations"
	"server/object/meta"
	"server/transactions"
)

type RenameObjectOperation struct {
	MetaDescription *meta.Meta
}

func (o *RenameObjectOperation) SyncMetaDescription(metaDescriptionToApply *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	if err := o.validate(metaDescriptionToApply, metaDescriptionSyncer); err != nil {
		return nil, err
	}

	//remove old description
	metaDescriptionSyncer.Remove(metaDescriptionToApply.Name)
	//create new one
	metaDescriptionSyncer.Create(transaction, o.MetaDescription.Name, o.MetaDescription.ForExport())

	return o.MetaDescription, nil
}

func (o *RenameObjectOperation) validate(metaObj *meta.Meta, metaDescriptionSyncer meta.MetaDescriptionSyncer) error {
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
