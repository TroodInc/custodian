package operations

import (
	"server/migrations/operations"
	"server/migrations"
	"server/object/meta"
	"fmt"
	"server/pg/migrations/operations/field"
	"server/migrations/description"
	"server/pg/migrations/operations/object"
)

type OperationFactory struct {
	metaFactory *meta.MetaFactory
}

func (of *OperationFactory) Factory(operationDescription *description.MigrationOperationDescription, metaObj *meta.Meta) (operations.MigrationOperation, error) {
	switch operationDescription.Type {
	case description.AddFieldOperation:
		if fieldDescription, err := of.metaFactory.FactoryFieldDescription(operationDescription.Field.Field, metaObj); err != nil {
			return nil, err
		} else {
			return field.NewAddFieldOperation(fieldDescription), nil
		}
	case description.RemoveFieldOperation:
		targetField := metaObj.FindField(operationDescription.Field.Name)
		if targetField == nil {
			return nil, migrations.NewMigrationError(fmt.Sprintf("meta %s has no field %s", metaObj.Name, operationDescription.Field.Name))
		}
		return field.NewRemoveFieldOperation(targetField), nil
	case description.UpdateFieldOperation:
		newField, err := of.metaFactory.FactoryFieldDescription(operationDescription.Field.Field, metaObj)
		if err != nil {
			return nil, err
		}
		currentField := metaObj.FindField(operationDescription.Field.Name)
		if currentField == nil {
			return nil, migrations.NewMigrationError(fmt.Sprintf("meta %s has no field %s", metaObj.Name, operationDescription.Field.Name))
		}
		return field.NewUpdateFieldOperation(currentField, newField), nil
	case description.CreateObjectOperation:
		metaObj, err := of.metaFactory.FactoryMeta(&operationDescription.MetaDescription)
		if err != nil {
			return nil, err
		}
		return object.NewCreateObjectOperation(metaObj), nil
	case description.RenameObjectOperation:
		metaObj, err := of.metaFactory.FactoryMeta(&operationDescription.MetaDescription)
		if err != nil {
			return nil, err
		}
		return object.NewRenameObjectOperation(metaObj), nil
	case description.DeleteObjectOperation:
		return object.NewDeleteObjectOperation(), nil
	}
	return nil, migrations.NewMigrationError(fmt.Sprintf(fmt.Sprintf("unknown type of operation(%s)", operationDescription.Type), metaObj.Name, operationDescription.Field.Name))
}

func NewOperationFactory(metaFactory *meta.MetaFactory) *OperationFactory {
	return &OperationFactory{metaFactory: metaFactory}
}
