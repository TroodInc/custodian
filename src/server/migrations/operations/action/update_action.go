package action

import (
	"server/data/notifications"
	"server/errors"
	"server/object"
	"server/object/meta"
	"server/transactions"
)

type UpdateActionOperation struct {
	NewAction     *notifications.Action
	CurrentAction *notifications.Action
}

func (o *UpdateActionOperation) SyncMetaDescription(metaDescriptionToApply *meta.Meta, transaction transactions.MetaDescriptionTransaction, syncer *object.Store) (*meta.Meta, error) {
	metaDescriptionToApply = metaDescriptionToApply.Clone()

	//replace action
	for i, action := range metaDescriptionToApply.Actions {
		if action.Name == o.CurrentAction.Name {
			metaDescriptionToApply.Actions[i] = o.NewAction
		}
	}

	//sync its MetaDescription
	if meta := syncer.Update(metaDescriptionToApply); meta == nil {
		return nil, errors.NewValidationError("", "Can't update action from migration", o.NewAction)
	} else {
		return meta, nil
	}
}

func (o *UpdateActionOperation) SyncDbDescription(metaDescriptionToApply *meta.Meta, transaction transactions.DbTransaction) (err error) {
	return nil
}

func NewUpdateActionOperation(currentAction, newAction *notifications.Action) *UpdateActionOperation {
	return &UpdateActionOperation{CurrentAction: currentAction, NewAction: newAction}
}
