package object

import (
	"server/object/meta"
	"server/transactions"
	"server/migrations"
	"fmt"
)

type RenameObjectOperation struct {
	NewName string
}

func (o *RenameObjectOperation) SyncMetaDescription(metaObj *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	if err := o.validate(metaObj, metaDescriptionSyncer); err != nil {
		return nil, err
	}
	metaDescription := metaObj.MetaDescription.Clone()
	metaDescription.Name = o.NewName

	//remove old description
	metaDescriptionSyncer.Remove(metaObj.Name)
	//create new one
	metaDescriptionSyncer.Create(transaction, *metaDescription)

	return meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(metaDescription)
}

func (o *RenameObjectOperation) validate(metaObj *meta.Meta, metaDescriptionSyncer meta.MetaDescriptionSyncer) error {
	metaDescription, _, _ := metaDescriptionSyncer.Get(o.NewName)
	if metaDescription != nil {
		return migrations.NewMigrationError(fmt.Sprintf("failed to rename object '%s' to '%s': object named '%s' already exists¬", metaObj.Name, o.NewName, o.NewName))
	}
	return nil
}
