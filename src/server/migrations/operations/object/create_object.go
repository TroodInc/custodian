package object

import (
	"server/object/meta"
	"server/transactions"
)

type CreateObjectOperation struct {
	MetaDescription *meta.Meta
}

func (o *CreateObjectOperation) SyncMetaDescription(metaDescriptionToApply *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	//sync its MetaDescription
	if err := metaDescriptionSyncer.Create(transaction, *o.MetaDescription); err != nil {
		return nil, err
	} else {
		return o.MetaDescription, nil
	}
}
