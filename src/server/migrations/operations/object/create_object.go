package object

import (
	"server/object/meta"
	"server/transactions"
)

type CreateObjectOperation struct {
	Meta *meta.Meta
}

func (o *CreateObjectOperation) SyncMetaDescription(applyToMetaObj *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {

	//sync its MetaDescription
	if err := metaDescriptionSyncer.Create(transaction, *o.Meta.MetaDescription); err != nil {
		return nil, err
	} else {
		return o.Meta, nil
	}
}
