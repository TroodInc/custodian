package field

import (
	"server/object"
	"server/transactions"
)

type UpdateFieldOperation struct {
	NewField     *object.Field
	CurrentField *object.Field
}

func (o *UpdateFieldOperation) SyncMetaDescription(metaDescriptionToApply *object.Meta, transaction transactions.MetaDescriptionTransaction, syncer object.MetaDescriptionSyncer) (*object.Meta, error) {
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

func NewUpdateFieldOperation(currentField *object.Field, newField *object.Field) *UpdateFieldOperation {
	return &UpdateFieldOperation{CurrentField: currentField, NewField: newField}
}
