package constructor

import (
	"server/errors"
	. "server/migrations/description"
	"server/object/description"
	"utils"
	"server/pg/migrations/managers"
	"server/transactions"
	"server/migrations"
	"reflect"
)

type MigrationConstructor struct {
	migrationManager *managers.MigrationManager
}

func (mc *MigrationConstructor) Construct(currentMetaDescription *description.MetaDescription, newMigrationMetaDescription *MigrationMetaDescription, transaction transactions.DbTransaction) (*MigrationDescription, error) {
	operationDescriptions := make([]MigrationOperationDescription, 0)

	if operationDescription := mc.processObjectCreation(currentMetaDescription, newMigrationMetaDescription); operationDescription != nil {
		operationDescriptions = append(operationDescriptions, *operationDescription)
	}

	if operationDescription := mc.processObjectRenaming(currentMetaDescription, newMigrationMetaDescription); operationDescription != nil {
		operationDescriptions = append(operationDescriptions, *operationDescription)
	}

	if operationDescription := mc.processObjectDeletion(currentMetaDescription, newMigrationMetaDescription); operationDescription != nil {
		operationDescriptions = append(operationDescriptions, *operationDescription)
	}

	operationDescriptions = append(operationDescriptions, mc.processFieldsAddition(currentMetaDescription, newMigrationMetaDescription)...)
	operationDescriptions = append(operationDescriptions, mc.processFieldsUpdate(currentMetaDescription, newMigrationMetaDescription)...)
	operationDescriptions = append(operationDescriptions, mc.processFieldsRemoval(currentMetaDescription, newMigrationMetaDescription)...)

	operationDescriptions = append(operationDescriptions, mc.processActionsAddition(currentMetaDescription, newMigrationMetaDescription)...)
	operationDescriptions = append(operationDescriptions, mc.processActionsUpdate(currentMetaDescription, newMigrationMetaDescription)...)
	operationDescriptions = append(operationDescriptions, mc.processActionsRemoval(currentMetaDescription, newMigrationMetaDescription)...)

	if len(operationDescriptions) == 0 {
		return nil, errors.NewValidationError(migrations.MigrationNoChangesWereDetected, "No changes were detected", nil)
	}

	var applyTo string
	if currentMetaDescription != nil {
		applyTo = currentMetaDescription.Name
	} else {
		applyTo = ""
	}

	var metaName string
	if currentMetaDescription != nil {
		metaName = currentMetaDescription.Name
	} else {
		metaName = newMigrationMetaDescription.Name
	}

	precedingMigrations, err := mc.migrationManager.GetPrecedingMigrationsForObject(metaName, transaction)
	if err != nil {
		return nil, err
	}
	dependsOn := make([]string, 0)
	for _, precedingMigration := range precedingMigrations {
		dependsOn = append(dependsOn, precedingMigration.Data["migration_id"].(string))
	}

	migrationDescription := MigrationDescription{
		Id:         utils.RandomString(8),
		ApplyTo:    applyTo,
		DependsOn:  dependsOn,
		Operations: operationDescriptions,
	}
	return &migrationDescription, nil
}

func (mc *MigrationConstructor) processObjectCreation(currentMetaDescription *description.MetaDescription, newMigrationMetaDescription *MigrationMetaDescription) *MigrationOperationDescription {
	if currentMetaDescription == nil && newMigrationMetaDescription != nil {
		return NewMigrationOperationDescription(CreateObjectOperation, nil, newMigrationMetaDescription.MetaDescription(), nil)
	}
	return nil
}

func (mc *MigrationConstructor) processObjectRenaming(currentMetaDescription *description.MetaDescription, newMigrationMetaDescription *MigrationMetaDescription) *MigrationOperationDescription {
	if currentMetaDescription != nil && newMigrationMetaDescription != nil {
		if currentMetaDescription.Name != newMigrationMetaDescription.Name {
			return NewMigrationOperationDescription(RenameObjectOperation, nil, newMigrationMetaDescription.MetaDescription(), nil)
		}
	}
	return nil
}

func (mc *MigrationConstructor) processObjectDeletion(currentMetaDescription *description.MetaDescription, newMigrationMetaDescription *MigrationMetaDescription) *MigrationOperationDescription {
	if currentMetaDescription != nil && newMigrationMetaDescription == nil {
		return NewMigrationOperationDescription(DeleteObjectOperation, nil, currentMetaDescription, nil)
	}
	return nil
}

func (mc *MigrationConstructor) processFieldsAddition(currentMetaDescription *description.MetaDescription, newMigrationMetaDescription *MigrationMetaDescription) []MigrationOperationDescription {
	operationDescriptions := make([]MigrationOperationDescription, 0)
	if currentMetaDescription != nil && newMigrationMetaDescription != nil {
		for i, migrationField := range newMigrationMetaDescription.Fields {
			//if field is not presented in the current metaDescription and is not supposed to be renamed
			if currentMetaDescription.FindField(migrationField.Name) == nil && migrationField.PreviousName == "" {
				operationDescriptions = append(operationDescriptions, *NewMigrationOperationDescription(AddFieldOperation, &newMigrationMetaDescription.Fields[i], nil, nil))
			}
		}
	}
	return operationDescriptions
}

func (mc *MigrationConstructor) processFieldsRemoval(currentMetaDescription *description.MetaDescription, newMigrationMetaDescription *MigrationMetaDescription) []MigrationOperationDescription {
	operationDescriptions := make([]MigrationOperationDescription, 0)
	if currentMetaDescription != nil && newMigrationMetaDescription != nil {
		for i, currentMetaField := range currentMetaDescription.Fields {
			//if field is not presented in the new metaDescription and is not supposed to be renamed
			if newMigrationMetaDescription.MetaDescription().FindField(currentMetaField.Name) == nil {
				if newMigrationMetaDescription.FindFieldWithPreviousName(currentMetaField.Name) == nil {
					operationDescriptions = append(operationDescriptions, *NewMigrationOperationDescription(RemoveFieldOperation, &MigrationFieldDescription{Field: currentMetaDescription.Fields[i], PreviousName: ""}, nil, nil))
				}
			}
		}
	}
	return operationDescriptions
}

func (mc *MigrationConstructor) processFieldsUpdate(currentMetaDescription *description.MetaDescription, newMigrationMetaDescription *MigrationMetaDescription) []MigrationOperationDescription {
	operationDescriptions := make([]MigrationOperationDescription, 0)
	if currentMetaDescription != nil && newMigrationMetaDescription != nil {
		for i, newFieldDescription := range newMigrationMetaDescription.Fields {
			var fieldName string
			if newFieldDescription.PreviousName != "" {
				fieldName = newFieldDescription.PreviousName
			} else {
				fieldName = newFieldDescription.Name
			}
			currentField := currentMetaDescription.FindField(fieldName)
			if currentField != nil {
				nameChanged := currentField.Name != newFieldDescription.Name
				defChanged := !reflect.DeepEqual(currentField.Def, newFieldDescription.Def)
				onDeleteChanged := currentField.OnDelete != newFieldDescription.OnDelete
				linkMetaListChanged := len(currentField.LinkMetaList.Diff(newFieldDescription.LinkMetaList)) > 0 || len(newFieldDescription.LinkMetaList.Diff(currentField.LinkMetaList)) > 0
				optionalChanged := currentField.Optional != newFieldDescription.Optional
				nowOnUpdateChanged := currentField.NowOnUpdate != newFieldDescription.NowOnUpdate
				nowOnCreateChanged := currentField.NowOnCreate != newFieldDescription.NowOnCreate
				if nameChanged || defChanged || onDeleteChanged || linkMetaListChanged || optionalChanged || nowOnCreateChanged || nowOnUpdateChanged {
					operationDescriptions = append(operationDescriptions, *NewMigrationOperationDescription(UpdateFieldOperation, &newMigrationMetaDescription.Fields[i], nil, nil))
				}
			}
		}
	}
	return operationDescriptions
}

func (mc *MigrationConstructor) processActionsAddition(currentMetaDescription *description.MetaDescription, newMigrationMetaDescription *MigrationMetaDescription) []MigrationOperationDescription {
	operationDescriptions := make([]MigrationOperationDescription, 0)
	if currentMetaDescription != nil && newMigrationMetaDescription != nil {
		for i, migrationAction := range newMigrationMetaDescription.Actions {
			//if action is not presented in the current metaDescription and is not supposed to be renamed
			if currentMetaDescription.FindAction(migrationAction.Name) == nil && migrationAction.PreviousName == "" {
				operationDescriptions = append(operationDescriptions, *NewMigrationOperationDescription(AddActionOperation, nil, nil, &newMigrationMetaDescription.Actions[i]))
			}
		}
	}
	return operationDescriptions
}

func (mc *MigrationConstructor) processActionsRemoval(currentMetaDescription *description.MetaDescription, newMigrationMetaDescription *MigrationMetaDescription) []MigrationOperationDescription {
	operationDescriptions := make([]MigrationOperationDescription, 0)
	if currentMetaDescription != nil && newMigrationMetaDescription != nil {
		for i, currentAction := range currentMetaDescription.Actions {
			//if action is not presented in the new metaDescription and is not supposed to be renamed
			if newMigrationMetaDescription.MetaDescription().FindAction(currentAction.Name) == nil {
				if newMigrationMetaDescription.FindActionWithPreviousName(currentAction.Name) == nil {
					operationDescriptions = append(operationDescriptions, *NewMigrationOperationDescription(RemoveActionOperation, nil, nil, &MigrationActionDescription{Action: currentMetaDescription.Actions[i]}))
				}
			}
		}
	}
	return operationDescriptions
}

func (mc *MigrationConstructor) processActionsUpdate(currentMetaDescription *description.MetaDescription, newMigrationMetaDescription *MigrationMetaDescription) []MigrationOperationDescription {
	operationDescriptions := make([]MigrationOperationDescription, 0)
	if currentMetaDescription != nil && newMigrationMetaDescription != nil {
		for i, newActionDescription := range newMigrationMetaDescription.Actions {
			var actionName string
			if newActionDescription.PreviousName != "" {
				actionName = newActionDescription.PreviousName
			} else {
				actionName = newActionDescription.Name
			}
			currentAction := currentMetaDescription.FindAction(actionName)
			if currentAction != nil {
				methodChanged := currentAction.Method != newActionDescription.Method
				protocolChanged := currentAction.Protocol != newActionDescription.Protocol
				argsChanged := !reflect.DeepEqual(currentAction.Args, newActionDescription.Args)
				activeIfNotRootChanged := currentAction.ActiveIfNotRoot != newActionDescription.ActiveIfNotRoot
				includeValuesChanged := !reflect.DeepEqual(currentAction.IncludeValues, newActionDescription.IncludeValues)
				nameChanged := currentAction.Name != newActionDescription.Name
				if nameChanged || methodChanged || protocolChanged || argsChanged || activeIfNotRootChanged || includeValuesChanged {
					operationDescriptions = append(operationDescriptions, *NewMigrationOperationDescription(UpdateActionOperation, nil, nil, &newMigrationMetaDescription.Actions[i]))
				}
			}
		}
	}
	return operationDescriptions
}

func NewMigrationConstructor(manager *managers.MigrationManager) *MigrationConstructor {
	return &MigrationConstructor{migrationManager: manager}
}
