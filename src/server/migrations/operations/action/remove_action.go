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

type RemoveActionOperation struct {
	Action *notifications.Action
}

func (o *RemoveActionOperation) SyncMetaDescription(metaDescription *meta.Meta, transaction transactions.MetaDescriptionTransaction, metaDescriptionSyncer *object.Store) (*meta.Meta, error) {
	updatedMetaDescription := metaDescription.Clone()
	if err := o.validate(updatedMetaDescription); err != nil {
		return nil, err
	}

	updatedMetaDescription.Actions = make([]*notifications.Action, 0)

	//remove action from the meta description
	for i, currentAction := range metaDescription.Actions {
		if currentAction.Name != o.Action.Name {
			updatedMetaDescription.Actions = append(updatedMetaDescription.Actions, metaDescription.Actions[i])
		}
	}
	//sync its MetaDescription
	if meta := metaDescriptionSyncer.Update(updatedMetaDescription); meta == nil {
		return nil, errors.NewValidationError("", "Cant remove action with migration", o.Action)
	} else {
		return meta, nil
	}
}

func (o *RemoveActionOperation) validate(metaDescription *meta.Meta) error {
	if metaDescription.FindAction(o.Action.Name) == nil {
		return errors.NewValidationError(
			migrations.MigrationErrorInvalidDescription,
			fmt.Sprintf("Object %s has no action named %s", metaDescription.Name, o.Action.Name),
			nil,
		)
	}
	return nil
}

func (o *RemoveActionOperation) SyncDbDescription(metaDescriptionToApply *meta.Meta, transaction transactions.DbTransaction) (err error) {
	return nil
}

func NewRemoveActionOperation(action *notifications.Action) *RemoveActionOperation {
	return &RemoveActionOperation{Action: action}
}
