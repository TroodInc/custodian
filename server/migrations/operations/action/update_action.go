package action

import (
	"custodian/server/object"
	meta_description "custodian/server/object/description"
	"custodian/server/transactions"
)

type UpdateActionOperation struct {
	NewAction     *meta_description.Action
	CurrentAction *meta_description.Action
}

func (o *UpdateActionOperation) SyncMetaDescription(metaDescriptionToApply *meta_description.MetaDescription, syncer object.MetaDescriptionSyncer) (*meta_description.MetaDescription, error) {
	metaDescriptionToApply = metaDescriptionToApply.Clone()

	//replace action
	for i, action := range metaDescriptionToApply.Actions {
		if action.Name == o.CurrentAction.Name {
			metaDescriptionToApply.Actions[i] = *o.NewAction
		}
	}

	//sync its MetaDescription
	if _, err := syncer.Update(metaDescriptionToApply.Name, *metaDescriptionToApply); err != nil {
		return nil, err
	} else {
		return metaDescriptionToApply, nil
	}
}

func (o *UpdateActionOperation) SyncDbDescription(metaDescriptionToApply *meta_description.MetaDescription, transaction transactions.DbTransaction, syncer object.MetaDescriptionSyncer) (err error) {
	return nil
}

func NewUpdateActionOperation(currentAction, newAction *meta_description.Action) *UpdateActionOperation {
	return &UpdateActionOperation{CurrentAction: currentAction, NewAction: newAction}
}
