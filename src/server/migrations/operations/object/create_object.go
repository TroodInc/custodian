package object

import (
	"server/object/description"
	"server/object/meta"
	"server/transactions"
)

type CreateObjectOperation struct {
	description.MetaDescription
}

func (o *CreateObjectOperation) SyncMetaDescription(_ *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	//factory new Meta
	metaObj, err := new(meta.MetaFactory).FactoryMeta(&o.MetaDescription)
	if err != nil {
		return metaObj, nil
	}
	//sync its MetaDescription
	if err = metaDescriptionSyncer.Create(transaction, *metaObj.MetaDescription); err != nil {
		return nil, err
	} else {
		return metaObj, nil
	}
}
