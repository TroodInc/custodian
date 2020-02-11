package object

import (
	"server/object/meta"
	"server/transactions"
)

type DeleteObjectOperation struct{}

func (o *DeleteObjectOperation) SyncMetaDescription(metaDescriptionToApply *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Remove(metaDescriptionToApply.Name); err != nil {
		return nil, err
	} else {
		return nil, nil
	}
}
