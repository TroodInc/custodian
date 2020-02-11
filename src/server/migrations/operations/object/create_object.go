package object

import (
	"server/object"
	"server/transactions"
)

type CreateObjectOperation struct {
	MetaDescription *object.Meta
}

func (o *CreateObjectOperation) SyncMetaDescription(metaDescriptionToApply *object.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer object.MetaDescriptionSyncer) (*object.Meta, error) {
	//sync its MetaDescription
	if err := metaDescriptionSyncer.Create(transaction, o.MetaDescription.Name, o.MetaDescription.ForExport()); err != nil {
		return nil, err
	} else {
		return o.MetaDescription, nil
	}
}
