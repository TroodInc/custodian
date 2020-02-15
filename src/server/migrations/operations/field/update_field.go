package field

import (
	"server/errors"
	"server/object"
	"server/object/meta"
	"server/transactions"
)

type UpdateFieldOperation struct {
	NewField     *meta.Field
	CurrentField *meta.Field
}

func (o *UpdateFieldOperation) SyncMetaDescription(metaDescriptionToApply *meta.Meta, transaction transactions.MetaDescriptionTransaction, syncer *object.Store) (*meta.Meta, error) {
	metaDescriptionToApply = metaDescriptionToApply.Clone()

	//replace field
	for i, field := range metaDescriptionToApply.Fields {
		if field.Name == o.CurrentField.Name {
			metaDescriptionToApply.Fields[i] = o.NewField
		}
	}

	//sync its MetaDescription
	if meta := syncer.Update(metaDescriptionToApply); meta == nil {
		return nil, errors.NewValidationError("", "Cant update field with migration", o.NewField)
	} else {
		return metaDescriptionToApply, nil
	}
}

func NewUpdateFieldOperation(currentField *meta.Field, newField *meta.Field) *UpdateFieldOperation {
	return &UpdateFieldOperation{CurrentField: currentField, NewField: newField}
}
