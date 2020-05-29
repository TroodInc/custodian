package validators

import (
	"fmt"
	errors2 "server/errors"
	"server/object/meta"
	"server/data/types"
	"server/data/errors"
	"server/transactions"
	"server/data/record"
)

type GenericInnerFieldValidator struct {
	metaGetCallback   func(name string, useCache bool) (*meta.Meta, bool, error)
	recordGetCallback func(objectClass, key string, ip []string, ep []string, depth int, omitOuters bool) (*record.Record, error)
	dbTransaction     transactions.DbTransaction
}

// Validates the generic inner field
func (validator *GenericInnerFieldValidator) Validate(fieldDescription *meta.FieldDescription, value interface{}) (*types.GenericInnerLink, error) {
	if castValue, ok := value.(map[string]interface{}); !ok {
		return nil, errors2.NewValidationError(
			errors.ErrWrongFiledType, fmt.Sprintf("NewField '%s' has a wrong type", fieldDescription.Name),
			map[string]string{"field": fieldDescription.Name},
		)
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

// Validates the object name generic inner field
func (validator *GenericInnerFieldValidator) validateObjectName(objectName interface{}, fieldDescription *meta.FieldDescription) (string, error) {
	castObjectName, ok := objectName.(string)
	if !ok {
		return "", errors2.NewValidationError(
			errors.ErrWrongFiledType,
			fmt.Sprintf("Generic field '%s' contains a wrong object name in its value", fieldDescription.Name),
			map[string]string{"field": fieldDescription.Name},
		)
	}
	return castObjectName, nil
}

// Validates the object generic inner field
func (validator *GenericInnerFieldValidator) validateObject(objectName string, fieldDescription *meta.FieldDescription) (*meta.Meta, error) {
	if objectMeta, _, err := validator.metaGetCallback(objectName, true); err != nil {
		return nil, errors2.NewValidationError(
			errors.ErrWrongFiledType, fmt.Sprintf("Object '%s' referenced in '%s'`s value does not exist", fieldDescription.Name),
			map[string]string{"field": fieldDescription.Name},
		)
	} else {
		return objectMeta, nil
	}
}

// Validates the record primary key of generic inner field
func (validator *GenericInnerFieldValidator) validateRecordPk(pkValue interface{}, fieldDescription *meta.FieldDescription) (interface{}, error) {
	if pkValue == nil {
		return nil, errors.GenericFieldPkIsNullError{}
	}
	var validatedPkValue interface{}
	switch castPkValue := pkValue.(type) {
	case float64, string:
		validatedPkValue = castPkValue
	case int:
		validatedPkValue = float64(castPkValue)
	default:
		return "", errors2.NewValidationError(
			errors.ErrWrongFiledType,
			fmt.Sprintf("PK value referenced in '%s'`s has wrong type", fieldDescription.Name),
			map[string]string{"field": fieldDescription.Name},
		)
	}
	return validatedPkValue, nil
}

// Validates the record of generic inner field record
func (validator *GenericInnerFieldValidator) validateRecord(objectMeta *meta.Meta, pkValue interface{}, fieldDescription *meta.FieldDescription) (error) {
	if pkValueAsString, err := objectMeta.Key.ValueAsString(pkValue); err != nil {
		return err
	} else {
		if recordData, err := validator.recordGetCallback(objectMeta.Name, pkValueAsString, nil, nil, 1, true); err != nil || recordData == nil {
			if err != nil {
				return errors2.NewValidationError(
					errors.ErrWrongFiledType,
					fmt.Sprintf("Failed to validate generic value of object '%s' with PK '%s' referenced in '%s'. Original error is: '%s'", objectMeta.Name, pkValue, fieldDescription.Name, err.Error()),
					map[string]string{"field": fieldDescription.Name},
				)
			} else {
				return errors2.NewValidationError(
					errors.ErrWrongFiledType, fmt.Sprintf("Record of object '%s' with PK '%s' referenced in '%s'`s value does not exist", objectMeta.Name, pkValue, fieldDescription.Name),
					map[string]string{"field": fieldDescription.Name},
				)
			}
		} else {
			return nil
		}
	}
}

func NewGenericInnerFieldValidator(dbTransaction transactions.DbTransaction, metaGetCallback func(name string, useCache bool) (*meta.Meta, bool, error), recordGetCallback func(objectClass, key string, ip []string, ep []string, depth int, omitOuters bool) (*record.Record, error)) *GenericInnerFieldValidator {
	return &GenericInnerFieldValidator{metaGetCallback: metaGetCallback, recordGetCallback: recordGetCallback, dbTransaction: dbTransaction}
}
