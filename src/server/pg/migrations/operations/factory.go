package operations

import (
	"fmt"
	"server/errors"
	"server/migrations"
	"server/migrations/description"
	"server/migrations/operations"
	"server/migrations/operations/action"
	"server/object/meta"
	"server/pg/migrations/operations/field"
	"server/pg/migrations/operations/object"
)

type OperationFactory struct{}

func (of *OperationFactory) Factory(migrationDescription *description.MigrationDescription, operationDescription *description.MigrationOperationDescription, metaDescription *meta.Meta) (operations.MigrationOperation, error) {
	switch operationDescription.Type {
	case description.AddFieldOperation:
		// migrationDescription.Id == "" indicated that current migration is added automatically and outer field should not be used for data retrieval
		if migrationDescription.Id != "" && operationDescription.Field.LinkType == meta.LinkTypeOuter {
			operationDescription.Field.RetrieveMode = true
			operationDescription.Field.QueryMode = true
		}
		return field.NewAddFieldOperation(&operationDescription.Field.Field), nil
	case description.RemoveFieldOperation:
		targetField := metaDescription.FindField(operationDescription.Field.Name)
		if targetField == nil {
			return nil, errors.NewValidationError(migrations.MigrationErrorInvalidDescription, fmt.Sprintf("meta %s has no field %s", metaDescription.Name, operationDescription.Field.Name), nil)
		}
		return field.NewRemoveFieldOperation(targetField), nil
	case description.UpdateFieldOperation:
		currentField := metaDescription.FindField(operationDescription.Field.PreviousName)
		if currentField == nil {
			return nil, errors.NewValidationError(migrations.MigrationErrorInvalidDescription, fmt.Sprintf("meta %s has no field %s", metaDescription.Name, operationDescription.Field.PreviousName), nil)
		}
		// migrationDescription.Id == "" indicated that current migration is added automatically and outer field should not be used for data retrieval
		if migrationDescription.Id != "" && operationDescription.Field.LinkType == meta.LinkTypeOuter {
			operationDescription.Field.RetrieveMode = true
			operationDescription.Field.QueryMode = true
		}
		return field.NewUpdateFieldOperation(currentField, &operationDescription.Field.Field), nil
	case description.CreateObjectOperation:
		return object.NewCreateObjectOperation(operationDescription.MetaDescription), nil
	case description.RenameObjectOperation:
		return object.NewRenameObjectOperation(operationDescription.MetaDescription), nil
	case description.DeleteObjectOperation:
		return object.NewDeleteObjectOperation(), nil
	case description.AddActionOperation:
		return action.NewAddActionOperation(&operationDescription.Action.Action), nil
	case description.UpdateActionOperation:
		currentAction := metaDescription.FindAction(operationDescription.Action.PreviousName)
		if currentAction == nil {
			return nil, errors.NewValidationError(migrations.MigrationErrorInvalidDescription, fmt.Sprintf("meta %s has no action %s", metaDescription.Name, operationDescription.Action.PreviousName), nil)
		}
		return action.NewUpdateActionOperation(currentAction, &operationDescription.Action.Action), nil
	case description.RemoveActionOperation:
		return action.NewRemoveActionOperation(&operationDescription.Action.Action), nil
	}
	return nil, errors.NewValidationError(migrations.MigrationErrorInvalidDescription, fmt.Sprintf(fmt.Sprintf("unknown type of operation(%s)", operationDescription.Type), metaDescription.Name, operationDescription), nil)
}

func NewOperationFactory() *OperationFactory {
	return &OperationFactory{}
}
