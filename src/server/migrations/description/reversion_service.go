package description

import (
	"fmt"
	"server/errors"
	"server/migrations"
	"server/object"
)

type ReversionMigrationDescriptionService struct{}

func (rmds *ReversionMigrationDescriptionService) Revert(previousStateMetaDescription *object.Meta, migrationDescription *MigrationDescription) (*MigrationDescription, error) {
	metaName, err := rmds.getMetaName(migrationDescription)
	if err != nil {
		return nil, err
	}
	backwardMigrationDescription := &MigrationDescription{
		Id:        migrationDescription.Id,
		ApplyTo:   metaName,
		DependsOn: migrationDescription.DependsOn,
	}

	for i := len(migrationDescription.Operations) - 1; i >= 0; i-- {
		invertedOperation, err := rmds.invertOperation(previousStateMetaDescription, &migrationDescription.Operations[i])
		if err != nil {
			return nil, err
		}
		backwardMigrationDescription.Operations = append(backwardMigrationDescription.Operations, *invertedOperation)
	}

	return backwardMigrationDescription, nil
}

func (rmds *ReversionMigrationDescriptionService) getMetaName(migrationDescription *MigrationDescription) (string, error) {
	metaName, err := migrationDescription.MetaName()
	if err != nil {
		return "", err
	}
	for _, operationDescription := range migrationDescription.Operations {
		if operationDescription.Type == RenameObjectOperation {
			metaName = operationDescription.MetaDescription.Name
		}

		if operationDescription.Type == DeleteObjectOperation {
			metaName = ""
		}
	}
	return metaName, err
}

func (rmds *ReversionMigrationDescriptionService) invertOperation(previousStateMetaDescription *object.Meta, operationDescription *MigrationOperationDescription) (*MigrationOperationDescription, error) {
	invertedOperation := &MigrationOperationDescription{}
	switch operationDescription.Type {
	case CreateObjectOperation:
		invertedOperation.Type = DeleteObjectOperation
		invertedOperation.MetaDescription = operationDescription.MetaDescription
	case RenameObjectOperation:
		invertedOperation.Type = RenameObjectOperation
		invertedOperation.MetaDescription = operationDescription.MetaDescription
		invertedOperation.MetaDescription.Name = previousStateMetaDescription.Name
	case DeleteObjectOperation:
		invertedOperation.Type = CreateObjectOperation
		invertedOperation.MetaDescription = operationDescription.MetaDescription
	case AddFieldOperation:
		invertedOperation.Type = RemoveFieldOperation
		invertedOperation.Field = operationDescription.Field
	case UpdateFieldOperation:
		previousField := previousStateMetaDescription.FindField(operationDescription.Field.PreviousName)
		if previousField == nil {
			return nil, errors.NewValidationError(migrations.MigrationErrorPreviousStateFieldNotFound, fmt.Sprintln("Failed to find previous state for field", operationDescription.Field.PreviousName), nil)
		}
		invertedOperation.Field = &MigrationFieldDescription{PreviousName: operationDescription.Field.Name, Field: *previousField}
		invertedOperation.Type = UpdateFieldOperation
	case RemoveFieldOperation:
		invertedOperation.Field = operationDescription.Field
		invertedOperation.Type = AddFieldOperation
	case AddActionOperation:
		invertedOperation.Type = RemoveActionOperation
		invertedOperation.Action = operationDescription.Action
	case UpdateActionOperation:
		previousAction := previousStateMetaDescription.FindAction(operationDescription.Action.PreviousName)
		if previousAction == nil {
			return nil, errors.NewValidationError(migrations.MigrationErrorPreviousStateActionNotFound, fmt.Sprintln("Failed to find previous state for action", operationDescription.Action.PreviousName), nil)
		}
		invertedOperation.Action = &MigrationActionDescription{PreviousName: operationDescription.Action.Name, Action: *previousAction}
		invertedOperation.Type = UpdateActionOperation
	case RemoveActionOperation:
		invertedOperation.Action = operationDescription.Action
		invertedOperation.Type = AddActionOperation
	}
	return invertedOperation, nil
}

func NewReversionMigrationDescriptionService() *ReversionMigrationDescriptionService {
	return &ReversionMigrationDescriptionService{}
}
