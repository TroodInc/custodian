package object

import (
	"server/object/meta"
	"server/transactions"
	"server/migrations"
	"fmt"
)

type RenameObjectOperation struct {
	Meta *meta.Meta
}

func (o *RenameObjectOperation) SyncMetaDescription(metaObj *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	if err := o.validate(metaObj, metaDescriptionSyncer); err != nil {
		return nil, err
	}

	//remove old description
	metaDescriptionSyncer.Remove(metaObj.Name)
	//create new one
	metaDescriptionSyncer.Create(transaction, *o.Meta.MetaDescription)

	return o.Meta, nil
}

func (o *RenameObjectOperation) validate(metaObj *meta.Meta, metaDescriptionSyncer meta.MetaDescriptionSyncer) error {
	metaDescription, _, _ := metaDescriptionSyncer.Get(o.Meta.Name)
	if metaDescription != nil {
		return migrations.NewMigrationError(fmt.Sprintf("failed to rename object '%s' to '%s': object named '%s' already exists", metaObj.Name, o.Meta.Name, o.Meta.Name))
	}
	return nil
}
