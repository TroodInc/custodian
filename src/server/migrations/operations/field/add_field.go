package field

import (
	"server/object/meta"
	meta_description "server/object/description"
	"server/transactions"
	"server/migrations"
	"fmt"
)

type AddFieldOperation struct {
	Field *meta_description.Field
}

func (o *AddFieldOperation) SyncMetaDescription(metaDescriptionToApply *meta_description.MetaDescription, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta_description.MetaDescription, error) {
	metaDescriptionToApply = metaDescriptionToApply.Clone()
	if err := o.validate(metaDescriptionToApply); err != nil {
		//TODO:This is a workaround to avoid duplicated outer field (<meta-name>_set) to be created.
		//This case is possible if some meta has 2 or more inner links to the same another meta
		if err.(*migrations.MigrationError).Code() == migrations.MigrationErrorDuplicated {
			return metaDescriptionToApply, nil
		} else {
			return nil, err
		}
	}
	fieldToAdd := o.Field.Clone()
	metaDescriptionToApply.Fields = append(metaDescriptionToApply.Fields, *fieldToAdd)

	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Update(metaDescriptionToApply.Name, *metaDescriptionToApply); err != nil {
		return nil, err
	} else {
		return metaDescriptionToApply, nil
	}
}

func (o *AddFieldOperation) validate(metaDescription *meta_description.MetaDescription) error {
	existingField := metaDescription.FindField(o.Field.Name)
	if existingField != nil {
		if existingField.LinkType == meta_description.LinkTypeOuter {
			if o.Field.LinkType == meta_description.LinkTypeOuter {
				if o.Field.OuterLinkField != existingField.OuterLinkField {
					return migrations.NewMigrationError(migrations.MigrationErrorDuplicated, "")
				}
			}
		}
		return migrations.NewMigrationError(
			migrations.MigrationErrorInvalidDescription,
			fmt.Sprintf("Object %s already has field %s", metaDescription.Name, o.Field.Name),
		)
	}
	return nil
}

func NewAddFieldOperation(field *meta_description.Field) *AddFieldOperation {
	return &AddFieldOperation{Field: field}
}
