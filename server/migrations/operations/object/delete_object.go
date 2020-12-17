package object

import (
	"custodian/server/object/meta"
	"custodian/server/object/description"
)

type DeleteObjectOperation struct{}

func (o *DeleteObjectOperation) SyncMetaDescription(metaDescriptionToApply *description.MetaDescription, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*description.MetaDescription, error) {
	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Remove(metaDescriptionToApply.Name); err != nil {
		return nil, err
	} else {
		return nil, nil
	}
}
