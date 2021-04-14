package object

import (
	"custodian/server/object"
	"custodian/server/object/description"
)

type CreateObjectOperation struct {
	MetaDescription *description.MetaDescription
}

func (o *CreateObjectOperation) SyncMetaDescription(metaDescriptionToApply *description.MetaDescription, metaDescriptionSyncer object.MetaDescriptionSyncer) (*description.MetaDescription, error) {
	//sync its MetaDescription
	if err := metaDescriptionSyncer.Create(*o.MetaDescription); err != nil {
		return nil, err
	} else {
		return o.MetaDescription, nil
	}
}
