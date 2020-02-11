package object

import (
	"server/object"
	"server/transactions"
)

type DeleteObjectOperation struct{}

func (o *DeleteObjectOperation) SyncMetaDescription(metaDescriptionToApply *object.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer object.MetaDescriptionSyncer) (*object.Meta, error) {
	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Remove(metaDescriptionToApply.Name); err != nil {
		return nil, err
	} else {
		return nil, nil
	}
}
