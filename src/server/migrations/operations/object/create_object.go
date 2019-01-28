package object

import (
	"server/object/meta"
	"server/transactions"
	"server/object/description"
)

type CreateObjectOperation struct {
	MetaDescription *description.MetaDescription
}

func (o *CreateObjectOperation) SyncMetaDescription(metaDescriptionToApply *description.MetaDescription, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*description.MetaDescription, error) {
	//sync its MetaDescription
	if err := metaDescriptionSyncer.Create(transaction, *o.MetaDescription); err != nil {
		return nil, err
	} else {
		return o.MetaDescription, nil
	}
}
