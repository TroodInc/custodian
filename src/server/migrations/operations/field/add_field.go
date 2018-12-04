package field

import (
	"server/object/meta"
	"server/transactions"
	"server/migrations"
	"fmt"
)

type AddFieldOperation struct {
	Field *meta.FieldDescription
}

func (o *AddFieldOperation) SyncMetaDescription(metaObj *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	if err := o.validate(metaObj); err != nil {
		return nil, err
	}

	metaDescription := metaObj.MetaDescription.Clone()
	metaDescription.Fields = append(metaDescription.Fields, *o.Field.Field)

	//factory new Meta
	metaObj, err := new(meta.MetaFactory).FactoryMeta(metaDescription)
	if err != nil {
		return metaObj, nil
	}
	//sync its MetaDescription
	if _, err = metaDescriptionSyncer.Update(metaObj.Name, *metaObj.MetaDescription); err != nil {
		return nil, err
	} else {
		return metaObj, nil
	}

	return new(meta.MetaFactory).FactoryMeta(metaDescription)
}

func (o *AddFieldOperation) validate(metaObj *meta.Meta) error {
	if metaObj.FindField(o.Field.Name) != nil {
		return migrations.NewMigrationError(fmt.Sprintf("Object %s already has field %s", metaObj.Name, o.Field.Name))
	}
	return nil
}
