package field

import (
	"server/object/meta"
	"server/transactions"
)

type UpdateFieldOperation struct {
	NewField     *meta.Field
	CurrentField *meta.Field
}

func (o *UpdateFieldOperation) SyncMetaDescription(metaDescriptionToApply *meta.Meta, transaction transactions.MetaDescriptionTransaction, syncer meta.MetaDescriptionSyncer) (*meta.Meta, error) {
	metaDescriptionToApply = metaDescriptionToApply.Clone()

	//replace field
	for i, field := range metaDescriptionToApply.Fields {
		if field.Name == o.CurrentField.Name {
			metaDescriptionToApply.Fields[i] = o.NewField
		}
	}

	//sync its MetaDescription
	if _, err := syncer.Update(metaDescriptionToApply.Name, metaDescriptionToApply.ForExport()); err != nil {
		return nil, err
	} else {
		return metaDescriptionToApply, nil
	}
}

func NewUpdateFieldOperation(currentField *meta.Field, newField *meta.Field) *UpdateFieldOperation {
	return &UpdateFieldOperation{CurrentField: currentField, NewField: newField}
}
