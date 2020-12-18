package action

import (
	"custodian/server/errors"
	"custodian/server/object/meta"
	"custodian/server/transactions"
	"custodian/server/migrations"
	"fmt"
	"custodian/server/object/description"
	meta_description "custodian/server/object/description"
)

type RemoveActionOperation struct {
	Action *description.Action
}

func (o *RemoveActionOperation) SyncMetaDescription(metaDescription *description.MetaDescription, metaDescriptionSyncer meta.MetaDescriptionSyncer) (*description.MetaDescription, error) {
	updatedMetaDescription := metaDescription.Clone()
	if err := o.validate(updatedMetaDescription); err != nil {
		return nil, err
	}

	updatedMetaDescription.Actions = make([]description.Action, 0)

	//remove action from the meta description
	for i, currentAction := range metaDescription.Actions {
		if currentAction.Name != o.Action.Name {
			updatedMetaDescription.Actions = append(updatedMetaDescription.Actions, metaDescription.Actions[i])
		}
	}
	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Update(updatedMetaDescription.Name, *updatedMetaDescription); err != nil {
		return nil, err
	} else {
		return updatedMetaDescription, nil
	}
}

func (o *RemoveActionOperation) validate(metaDescription *description.MetaDescription) error {
	if metaDescription.FindAction(o.Action.Name) == nil {
		return errors.NewValidationError(
			migrations.MigrationErrorInvalidDescription,
			fmt.Sprintf("Object %s has no action named %s", metaDescription.Name, o.Action.Name),
			nil,
		)
	}
	return nil
}

func (o *RemoveActionOperation) SyncDbDescription(metaDescriptionToApply *meta_description.MetaDescription, transaction transactions.DbTransaction, syncer meta.MetaDescriptionSyncer) (err error) {
	return nil
}

func NewRemoveActionOperation(action *description.Action) *RemoveActionOperation {
	return &RemoveActionOperation{Action: action}
}
