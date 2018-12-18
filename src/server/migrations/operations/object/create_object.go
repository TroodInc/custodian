package object

import (
	"server/object/meta"
	"server/transactions"
)

type CreateObjectOperation struct {
}

func (o *CreateObjectOperation) SyncMetaDescription(metaObj *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {

	//sync its MetaDescription
	if err := metaDescriptionSyncer.Create(transaction, *metaObj.MetaDescription); err != nil {
		return nil, err
	} else {
		return metaObj, nil
	}
}
