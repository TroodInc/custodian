package field

import (
	"server/object/meta"
	"server/transactions"
	"server/migrations"
	"fmt"
	"server/object/description"
)

type RemoveFieldOperation struct {
	Field *meta.FieldDescription
}

func (o *RemoveFieldOperation) SyncMetaDescription(metaObj *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	if err := o.validate(metaObj); err != nil {
		return nil, err
	}

	metaDescription := metaObj.MetaDescription.Clone()
	metaDescription.Fields = make([]description.Field, 0)

	//remove field from meta description
	for i, currentField := range metaObj.MetaDescription.Fields {
		if currentField.Name != o.Field.Name {
			metaDescription.Fields = append(metaDescription.Fields, metaObj.MetaDescription.Fields[i])
		}
	}
	//factory new Meta
	metaObj, err := meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(metaDescription)
	if err != nil {
		return metaObj, nil
	}
	//sync its MetaDescription
	if _, err = metaDescriptionSyncer.Update(metaObj.Name, *metaObj.MetaDescription); err != nil {
		return nil, err
	} else {
		return metaObj, nil
	}

	return meta.NewMetaFactory(metaDescriptionSyncer).FactoryMeta(metaDescription)
}

func (o *RemoveFieldOperation) validate(metaObj *meta.Meta) error {
	if metaObj.FindField(o.Field.Name) == nil {
		return migrations.NewMigrationError(fmt.Sprintf("Object %s has no field named %s", metaObj.Name, o.Field.Name))
	}
	return nil
}

func NewRemoveFieldOperation(field *meta.FieldDescription) *RemoveFieldOperation {
	return &RemoveFieldOperation{Field: field}
}
