package object

import (
	errors2 "custodian/server/errors"
	"custodian/server/object/errors"
	"fmt"
)

type GenericInnerFieldValidator struct {
	metaGetCallback   func(name string, useCache bool) (*Meta, bool, error)
	recordGetCallback func(objectClass, key string, ip []string, ep []string, depth int, omitOuters bool) (*Record, error)
}

func (validator *GenericInnerFieldValidator) Validate(fieldDescription *FieldDescription, value interface{}) (*GenericInnerLink, error) {
	if castValue, ok := value.(map[string]interface{}); !ok {
		return nil, errors2.NewValidationError(
			errors.ErrWrongFiledType, fmt.Sprintf("NewField '%s' has a wrong type", fieldDescription.Name),
			map[string]string{"field": fieldDescription.Name},
		)
	} else {
		if objectName, err := validator.validateObjectName(castValue[GenericInnerLinkObjectKey], fieldDescription); err != nil {
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
						return &GenericInnerLink{ObjectName: objectMeta.Name, Pk: pkValue, PkName: objectMeta.Key.Name, FieldDescription: objectMeta.Key}, nil
					}
				}
			}
		}
	}
}

func (validator *GenericInnerFieldValidator) validateObjectName(objectName interface{}, fieldDescription *FieldDescription) (string, error) {
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

func (validator *GenericInnerFieldValidator) validateObject(objectName string, fieldDescription *FieldDescription) (*Meta, error) {
	if objectMeta, _, err := validator.metaGetCallback(objectName, true); err != nil {
		return nil, errors2.NewValidationError(
			errors.ErrWrongFiledType, fmt.Sprintf("Object '%s' referenced in '%s'`s value does not exist", fieldDescription.Meta.Name, fieldDescription.Name),
			map[string]string{"field": fieldDescription.Name},
		)
	} else {
		return objectMeta, nil
	}
}

func (validator *GenericInnerFieldValidator) validateRecordPk(pkValue interface{}, fieldDescription *FieldDescription) (interface{}, error) {
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

func (validator *GenericInnerFieldValidator) validateRecord(objectMeta *Meta, pkValue interface{}, fieldDescription *FieldDescription) (error) {
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

func NewGenericInnerFieldValidator(metaGetCallback func(name string, useCache bool) (*Meta, bool, error), recordGetCallback func(objectClass, key string, ip []string, ep []string, depth int, omitOuters bool) (*Record, error)) *GenericInnerFieldValidator {
	return &GenericInnerFieldValidator{metaGetCallback: metaGetCallback, recordGetCallback: recordGetCallback}
}
