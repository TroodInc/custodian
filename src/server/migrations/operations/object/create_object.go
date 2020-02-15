package object

import (
	"server/errors"
	"server/object"
	"server/object/meta"
	"server/transactions"
)

type CreateObjectOperation struct {
	MetaDescription *meta.Meta
}

func (o *CreateObjectOperation) SyncMetaDescription(metaDescriptionToApply *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer *object.Store) (*meta.Meta, error) {
	//sync its MetaDescription
	if meta := metaDescriptionSyncer.Create(o.MetaDescription); meta == nil {
		//TODO: Add code here
		return nil, errors.NewValidationError("", "Cant create meta from migration", o.MetaDescription)
	} else {
		return meta, nil
	}
}
