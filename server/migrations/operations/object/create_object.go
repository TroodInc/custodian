package object

import (
	"custodian/server/object/meta"
	"custodian/server/transactions"
	"custodian/server/object/description"
)

type CreateObjectOperation struct {
	MetaDescription *description.MetaDescription
}

func (o *CreateObjectOperation) SyncMetaDescription(metaDescriptionToApply *description.MetaDescription, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*description.MetaDescription, error) {
	//sync its MetaDescription
	if err := metaDescriptionSyncer.Create(*o.MetaDescription); err != nil {
		return nil, err
	} else {
		return o.MetaDescription, nil
	}
}
