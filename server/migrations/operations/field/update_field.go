package field

import (
	"custodian/server/object/meta"
	"custodian/server/object/description"
)

type UpdateFieldOperation struct {
	NewField     *description.Field
	CurrentField *description.Field
}

func (o *UpdateFieldOperation) SyncMetaDescription(metaDescriptionToApply *description.MetaDescription, syncer meta.MetaDescriptionSyncer) (*description.MetaDescription, error) {
	metaDescriptionToApply = metaDescriptionToApply.Clone()

	//replace field
	for i, field := range metaDescriptionToApply.Fields {
		if field.Name == o.CurrentField.Name {
			metaDescriptionToApply.Fields[i] = *o.NewField
		}
	}

	//sync its MetaDescription
	if _, err := syncer.Update(metaDescriptionToApply.Name, *metaDescriptionToApply); err != nil {
		return nil, err
	} else {
		return metaDescriptionToApply, nil
	}
}

func NewUpdateFieldOperation(currentField *description.Field, newField *description.Field) *UpdateFieldOperation {
	return &UpdateFieldOperation{CurrentField: currentField, NewField: newField}
}
