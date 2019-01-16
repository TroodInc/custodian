package operations

import (
	"server/migrations/operations"
	"server/migrations"
	meta_description "server/object/description"
	"fmt"
	"server/pg/migrations/operations/field"
	"server/migrations/description"
	"server/pg/migrations/operations/object"
)

type OperationFactory struct{}

func (of *OperationFactory) Factory(operationDescription *description.MigrationOperationDescription, metaDescription *meta_description.MetaDescription) (operations.MigrationOperation, error) {
	switch operationDescription.Type {
	case description.AddFieldOperation:
		return field.NewAddFieldOperation(&operationDescription.Field.Field), nil
	case description.RemoveFieldOperation:
		targetField := metaDescription.FindField(operationDescription.Field.Name)
		if targetField == nil {
			return nil, migrations.NewMigrationError(fmt.Sprintf("meta %s has no field %s", metaDescription.Name, operationDescription.Field.Name))
		}
		return field.NewRemoveFieldOperation(targetField), nil
	case description.UpdateFieldOperation:
		currentField := metaDescription.FindField(operationDescription.Field.PreviousName)
		if currentField == nil {
			return nil, migrations.NewMigrationError(fmt.Sprintf("meta %s has no field %s", metaDescription.Name, operationDescription.Field.PreviousName))
		}
		return field.NewUpdateFieldOperation(currentField, &operationDescription.Field.Field), nil
	case description.CreateObjectOperation:
		return object.NewCreateObjectOperation(&operationDescription.MetaDescription), nil
	case description.RenameObjectOperation:
		return object.NewRenameObjectOperation(&operationDescription.MetaDescription), nil
	case description.DeleteObjectOperation:
		return object.NewDeleteObjectOperation(), nil
	}
	return nil, migrations.NewMigrationError(fmt.Sprintf(fmt.Sprintf("unknown type of operation(%s)", operationDescription.Type), metaDescription.Name, operationDescription.Field.Name))
}

func NewOperationFactory() *OperationFactory {
	return &OperationFactory{}
}
