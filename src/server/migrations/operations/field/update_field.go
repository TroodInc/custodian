package field

import (
	"server/object/meta"
	"server/transactions"
)

type UpdateFieldOperation struct {
	NewField     *meta.FieldDescription
	CurrentField *meta.FieldDescription
}

func (o *UpdateFieldOperation) SyncMetaDescription(metaObj *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	metaDescription := metaObj.MetaDescription.Clone()

	//replace field
	for i, field := range metaDescription.Fields {
		if field.Name == o.NewField.Name {
			metaDescription.Fields[i] = *o.NewField.Field
		}
	}

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
