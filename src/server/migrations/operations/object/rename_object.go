package object

import (
	"fmt"
	"server/errors"
	"server/migrations"
	"server/object"
	"server/object/meta"
	"server/transactions"
)

type RenameObjectOperation struct {
	MetaDescription *meta.Meta
}

func (o *RenameObjectOperation) SyncMetaDescription(metaDescriptionToApply *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer *object.Store) (*meta.Meta, error) {
	if err := o.validate(metaDescriptionToApply, metaDescriptionSyncer); err != nil {
		return nil, err
	}

	//remove old description
	metaDescriptionSyncer.Remove(metaDescriptionToApply.Name)
	//create new one
	metaDescriptionSyncer.Create(o.MetaDescription)

	return o.MetaDescription, nil
}

func (o *RenameObjectOperation) validate(metaObj *meta.Meta, metaDescriptionSyncer *object.Store) error {
	metaDescription := metaDescriptionSyncer.Get(o.MetaDescription.Name)
	if metaDescription != nil {
		return errors.NewValidationError(
			migrations.MigrationErrorInvalidDescription,
			fmt.Sprintf("failed to rename object '%s' to '%s': object named '%s' already exists", metaObj.Name, o.MetaDescription.Name, o.MetaDescription.Name),
			nil,
		)
	}
	return nil
}
