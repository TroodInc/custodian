package object

import (
	"server/object/meta"
	"server/transactions"
	"server/migrations"
	"fmt"
	"server/object/description"
)

type RenameObjectOperation struct {
	MetaDescription *description.MetaDescription
}

func (o *RenameObjectOperation) SyncMetaDescription(metaDescriptionToApply *description.MetaDescription, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*description.MetaDescription, error) {
	if err := o.validate(metaDescriptionToApply, metaDescriptionSyncer); err != nil {
		return nil, err
	}

	//remove old description
	metaDescriptionSyncer.Remove(metaDescriptionToApply.Name)
	//create new one
	metaDescriptionSyncer.Create(transaction, *o.MetaDescription)

	return o.MetaDescription, nil
}

func (o *RenameObjectOperation) validate(metaObj *description.MetaDescription, metaDescriptionSyncer meta.MetaDescriptionSyncer) error {
	metaDescription, _, _ := metaDescriptionSyncer.Get(o.MetaDescription.Name)
	if metaDescription != nil {
		return migrations.NewMigrationError(fmt.Sprintf("failed to rename object '%s' to '%s': object named '%s' already exists", metaObj.Name, o.MetaDescription.Name, o.MetaDescription.Name))
	}
	return nil
}
