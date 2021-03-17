package object

import (
	"custodian/server/errors"
	"custodian/server/migrations"
	"custodian/server/object"
	"custodian/server/object/description"
	"fmt"
)

type RenameObjectOperation struct {
	MetaDescription *description.MetaDescription
}

func (o *RenameObjectOperation) SyncMetaDescription(metaDescriptionToApply *description.MetaDescription, metaDescriptionSyncer object.MetaDescriptionSyncer) (*description.MetaDescription, error) {
	if err := o.validate(metaDescriptionToApply, metaDescriptionSyncer); err != nil {
		return nil, err
	}

	//remove old description
	metaDescriptionSyncer.Remove(metaDescriptionToApply.Name)

	//avoid empty fields
	newMetaDescription := metaDescriptionToApply.Clone()
	newMetaDescription.Name = o.MetaDescription.Name
	o.MetaDescription = newMetaDescription
	
	//create new one
	metaDescriptionSyncer.Create(*o.MetaDescription)

	return o.MetaDescription, nil
}

func (o *RenameObjectOperation) validate(metaObj *description.MetaDescription, metaDescriptionSyncer object.MetaDescriptionSyncer) error {
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
