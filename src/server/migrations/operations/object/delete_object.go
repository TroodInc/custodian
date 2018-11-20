package object

import (
	"server/object/meta"
	"server/transactions"
)

type DeleteObjectOperation struct{}

func (o *DeleteObjectOperation) SyncMetaDescription(metaObj *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Remove(metaObj.Name); err != nil {
		return nil, err
	} else {
		return nil, nil
	}
}
