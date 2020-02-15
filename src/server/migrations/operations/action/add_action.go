package action

import (
	"fmt"
	"server/data/notifications"
	"server/errors"
	"server/migrations"
	"server/object"
	"server/object/meta"
	"server/transactions"
)

type AddActionOperation struct {
	Action *notifications.Action
}

func (o *AddActionOperation) SyncMetaDescription(metaDescriptionToApply *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer *object.Store) (*meta.Meta, error) {
	metaDescriptionToApply = metaDescriptionToApply.Clone()
	if err := o.validate(metaDescriptionToApply); err != nil {
		return nil, err
	}
	actionToAdd := o.Action.Clone()

	metaDescriptionToApply.Actions = append(metaDescriptionToApply.Actions, actionToAdd)

	//sync its MetaDescription
	if meta := metaDescriptionSyncer.Update(metaDescriptionToApply); meta == nil {
		return nil, errors.NewValidationError("", "Cant add action from migration", o.Action)
	} else {
		return metaDescriptionToApply, nil
	}
}

func (o *AddActionOperation) validate(metaDescription *meta.Meta) error {
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

func (o *AddActionOperation) SyncDbDescription(metaDescriptionToApply *meta.Meta, transaction transactions.DbTransaction) (err error) {
	return nil
}

func NewAddActionOperation(action *notifications.Action) *AddActionOperation {
	return &AddActionOperation{Action: action}
}
