package validators

import (
	"server/object/meta"
	"server/data/types"
	"server/data/errors"
	"server/transactions"
	"server/data/record"
)

type GenericInnerFieldValidator struct {
	metaGetCallback   func(transaction *transactions.GlobalTransaction, name string) (*meta.Meta, bool, error)
	recordGetCallback func(transaction transactions.DbTransaction, objectClass, key string, depth int) (*record.Record, error)
	dbTransaction     transactions.DbTransaction
}

func (validator *GenericInnerFieldValidator) Validate(fieldDescription *meta.FieldDescription, value interface{}) (*types.GenericInnerLink, error) {
	if castValue, ok := value.(map[string]interface{}); !ok {
		return nil, errors.NewDataError(fieldDescription.Meta.Name, errors.ErrWrongFiledType, "Field '%s' has a wrong type", fieldDescription.Name)
	} else {
		if objectName, err := validator.validateObjectName(castValue[types.GenericInnerLinkObjectKey], fieldDescription); err != nil {
			return nil, err
		} else {
			if objectMeta, err := validator.validateObject(objectName, fieldDescription); err != nil {
				return nil, err
			} else {
				if pkValue, err := validator.validateRecordPk(castValue[objectMeta.Key.Name], fieldDescription); err != nil {
					return nil, err
				} else {
					if err := validator.validateRecord(objectMeta, pkValue, fieldDescription); err != nil {
						return nil, err
					} else {
						return &types.GenericInnerLink{ObjectName: objectMeta.Name, Pk: pkValue, PkName: objectMeta.Key.Name, FieldDescription: objectMeta.Key}, nil
					}
				}
			}
		}
	}
}

func (validator *GenericInnerFieldValidator) validateObjectName(objectName interface{}, fieldDescription *meta.FieldDescription) (string, error) {
	castObjectName, ok := objectName.(string)
	if !ok {
		return "", errors.NewDataError(fieldDescription.Meta.Name, errors.ErrWrongFiledType, "Generic field '%s' contains a wrong object name in its value", fieldDescription.Name)
	}
	return castObjectName, nil
}

func (validator *GenericInnerFieldValidator) validateObject(objectName string, fieldDescription *meta.FieldDescription) (*meta.Meta, error) {
	if objectMeta, _, err := validator.metaGetCallback(&transactions.GlobalTransaction{DbTransaction: validator.dbTransaction}, objectName); err != nil {
		return nil, errors.NewDataError(fieldDescription.Meta.Name, errors.ErrWrongFiledType, "Object '%s' referenced in '%s'`s value does not exist", fieldDescription.Name)
	} else {
		return objectMeta, nil
	}
}

func (validator *GenericInnerFieldValidator) validateRecordPk(pkValue interface{}, fieldDescription *meta.FieldDescription) (interface{}, error) {
	var validatedPkValue interface{}
	switch castPkValue := pkValue.(type) {
	case float64, string:
		validatedPkValue = castPkValue
	case int:
		validatedPkValue = float64(castPkValue)
	default:
		return "", errors.NewDataError(fieldDescription.Meta.Name, errors.ErrWrongFiledType, "PK value referenced in '%s'`s has wrong type", fieldDescription.Name)
	}
	return validatedPkValue, nil
}

func (validator *GenericInnerFieldValidator) validateRecord(objectMeta *meta.Meta, pkValue interface{}, fieldDescription *meta.FieldDescription) (error) {
	if pkValueAsString, err := objectMeta.Key.ValueAsString(pkValue); err != nil {
		return err
	} else {
		if recordData, err := validator.recordGetCallback(validator.dbTransaction, objectMeta.Name, pkValueAsString, 1); err != nil || recordData == nil {
			if err != nil {
				return errors.NewDataError(fieldDescription.Meta.Name, errors.ErrWrongFiledType, "Failed to validate generic value of object '%s' with PK '%s' referenced in '%s'. Original error is: '%s'", objectMeta.Name, pkValue, fieldDescription.Name, err.Error())
			} else {
				return errors.NewDataError(fieldDescription.Meta.Name, errors.ErrWrongFiledType, "Record of object '%s' with PK '%s' referenced in '%s'`s value does not exist", objectMeta.Name, pkValue, fieldDescription.Name)
			}
		} else {
			return nil
		}
	}
}

func NewGenericInnerFieldValidator(dbTransaction transactions.DbTransaction, metaGetCallback func(transaction *transactions.GlobalTransaction, name string) (*meta.Meta, bool, error), recordGetCallback func(transaction transactions.DbTransaction, objectClass, key string, depth int) (*record.Record, error)) *GenericInnerFieldValidator {
	return &GenericInnerFieldValidator{metaGetCallback: metaGetCallback, recordGetCallback: recordGetCallback, dbTransaction: dbTransaction}
}
