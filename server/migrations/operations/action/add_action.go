package action

import (
	"custodian/server/errors"
	"custodian/server/migrations"
	"custodian/server/object"
	meta_description "custodian/server/object/description"
	"custodian/server/transactions"

	"fmt"
)

type AddActionOperation struct {
	Action *meta_description.Action
}

func (o *AddActionOperation) SyncMetaDescription(metaDescriptionToApply *meta_description.MetaDescription, metaDescriptionSyncer object.MetaDescriptionSyncer) (*meta_description.MetaDescription, error) {
	metaDescriptionToApply = metaDescriptionToApply.Clone()
	if err := o.validate(metaDescriptionToApply); err != nil {
		return nil, err
	}
	actionToAdd := o.Action.Clone()

	metaDescriptionToApply.Actions = append(metaDescriptionToApply.Actions, *actionToAdd)

	//sync its MetaDescription
	if _, err := metaDescriptionSyncer.Update(metaDescriptionToApply.Name, *metaDescriptionToApply); err != nil {
		return nil, err
	} else {
		return metaDescriptionToApply, nil
	}
}

func (o *AddActionOperation) validate(metaDescription *meta_description.MetaDescription) error {
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

func (o *AddActionOperation) SyncDbDescription(metaDescriptionToApply *meta_description.MetaDescription, transaction transactions.DbTransaction, syncer object.MetaDescriptionSyncer) (err error) {
	return nil
}

func NewAddActionOperation(action *meta_description.Action) *AddActionOperation {
	return &AddActionOperation{Action: action}
}
