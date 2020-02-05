package field

import (
	"fmt"
	"server/errors"
	"server/migrations"
	"server/object/meta"
	"server/transactions"
)

type AddFieldOperation struct {
	Field *meta.Field
}

func (o *AddFieldOperation) SyncMetaDescription(metaDescriptionToApply *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	metaDescriptionToApply = metaDescriptionToApply.Clone()
	if err := o.validate(metaDescriptionToApply); err != nil {
		//TODO:This is a workaround to avoid duplicated outer field (<meta-name>_set) to be created.
		//This case is possible if some meta has 2 or more inner links to the same another meta
		if err.Code == migrations.MigrationErrorDuplicated {
			return metaDescriptionToApply, nil
		} else {
			return nil, err
		}
	}
	fieldToAdd := o.Field.Clone()
	metaDescriptionToApply.Fields = append(metaDescriptionToApply.Fields, fieldToAdd)

	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Update(metaDescriptionToApply.Name, metaDescriptionToApply.ForExport()); err != nil {
		return nil, err
	} else {
		return metaDescriptionToApply, nil
	}
}

func (o *AddFieldOperation) validate(metaDescription *meta.Meta) *errors.ServerError {
	existingField := metaDescription.FindField(o.Field.Name)
	if existingField != nil {
		if existingField.LinkType == meta.LinkTypeOuter {
			if o.Field.LinkType == meta.LinkTypeOuter {
				if o.Field.OuterLinkField != existingField.OuterLinkField {
					return errors.NewValidationError(migrations.MigrationErrorDuplicated, "", nil)
				}
			}
		}
		return errors.NewValidationError(
			migrations.MigrationErrorInvalidDescription,
			fmt.Sprintf("Object %s already has field %s", metaDescription.Name, o.Field.Name),
			nil,
		)
	}
	return nil
}

func NewAddFieldOperation(field *meta.Field) *AddFieldOperation {
	return &AddFieldOperation{Field: field}
}
