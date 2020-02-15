package object

import (
	"server/object"
	"server/object/meta"
	"server/transactions"
)

type DeleteObjectOperation struct{}

func (o *DeleteObjectOperation) SyncMetaDescription(metaDescriptionToApply *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer *object.Store) (*meta.Meta, error) {
	//sync its MetaDescription
	if err := metaDescriptionSyncer.Remove(metaDescriptionToApply.Name); err != nil {
		return nil, err
	} else {
		return nil, nil
	}
}
