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
		return nil, err
	}

	metaDescriptionToApply.Fields = append(metaDescriptionToApply.Fields, *o.Field)

	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Update(metaDescriptionToApply.Name, *metaDescriptionToApply); err != nil {
		return nil, err
	} else {
		return metaDescriptionToApply, nil
	}
}

func (o *AddFieldOperation) validate(metaDescription *meta_description.MetaDescription) error {
	if metaDescription.FindField(o.Field.Name) != nil {
		return migrations.NewMigrationError(fmt.Sprintf("Object %s already has field %s", metaDescription.Name, o.Field.Name))
	}
	return nil
}

func NewAddFieldOperation(field *meta_description.Field) *AddFieldOperation {
	return &AddFieldOperation{Field: field}
}
