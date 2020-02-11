package action

import (
	"server/data/notifications"
	"server/object"
	"server/transactions"
)

type UpdateActionOperation struct {
	NewAction     *notifications.Action
	CurrentAction *notifications.Action
}

func (o *UpdateActionOperation) SyncMetaDescription(metaDescriptionToApply *object.Meta, transaction transactions.MetaDescriptionTransaction, syncer object.MetaDescriptionSyncer) (*object.Meta, error) {
	metaDescriptionToApply = metaDescriptionToApply.Clone()

	//replace action
	for i, action := range metaDescriptionToApply.Actions {
		if action.Name == o.CurrentAction.Name {
			metaDescriptionToApply.Actions[i] = o.NewAction
		}
	}

	//sync its MetaDescription
	if _, err := syncer.Update(metaDescriptionToApply.Name, metaDescriptionToApply.ForExport()); err != nil {
		return nil, err
	} else {
		return metaDescriptionToApply, nil
	}
}

func (o *UpdateActionOperation) SyncDbDescription(metaDescriptionToApply *object.Meta, transaction transactions.DbTransaction, syncer object.MetaDescriptionSyncer) (err error) {
	return nil
}

func NewUpdateActionOperation(currentAction, newAction *notifications.Action) *UpdateActionOperation {
	return &UpdateActionOperation{CurrentAction: currentAction, NewAction: newAction}
}
