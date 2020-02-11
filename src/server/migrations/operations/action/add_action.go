package action

import (
	"fmt"
	"server/data/notifications"
	"server/errors"
	"server/migrations"
	"server/object"
	"server/transactions"
)

type AddActionOperation struct {
	Action *notifications.Action
}

func (o *AddActionOperation) SyncMetaDescription(metaDescriptionToApply *object.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer object.MetaDescriptionSyncer) (*object.Meta, error) {
	metaDescriptionToApply = metaDescriptionToApply.Clone()
	if err := o.validate(metaDescriptionToApply); err != nil {
		return nil, err
	}
	actionToAdd := o.Action.Clone()

	metaDescriptionToApply.Actions = append(metaDescriptionToApply.Actions, actionToAdd)

	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Update(metaDescriptionToApply.Name, metaDescriptionToApply.ForExport()); err != nil {
		return nil, err
	} else {
		return metaDescriptionToApply, nil
	}
}

func (o *AddActionOperation) validate(metaDescription *object.Meta) error {
	existingAction := metaDescription.FindAction(o.Action.Name)
	if existingAction != nil {
		return errors.NewValidationError(
			migrations.MigrationErrorInvalidDescription,
			fmt.Sprintf("Object %s already has action named '%s'", metaDescription.Name, o.Action.Name),
			nil,
		)
	}
	return nil
}

func (o *AddActionOperation) SyncDbDescription(metaDescriptionToApply *object.Meta, transaction transactions.DbTransaction, syncer object.MetaDescriptionSyncer) (err error) {
	return nil
}

func NewAddActionOperation(action *notifications.Action) *AddActionOperation {
	return &AddActionOperation{Action: action}
}
