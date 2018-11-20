package field

import (
	"server/object/description"
	"server/object/meta"
	"server/transactions"
)

type AddFieldOperation struct {
	Field *description.Field
}

func (o *AddFieldOperation) SyncMetaDescription(metaObj *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	metaDescription := metaObj.MetaDescription.Clone()
	metaDescription.Fields = append(metaDescription.Fields, *o.Field)
	//TODO: Implement processing of related fields(e.g. removal of outerLink on innerLink`s removal)
	return new(meta.MetaFactory).FactoryMeta(metaDescription)
}
