package validators

import (
	"server/meta"
	"server/data/types"
	"server/data/errors"
	"strconv"
)

type GenericInnerFieldValidator struct {
	metaGetCallback func(name string, handleTransaction bool) (*meta.Meta, bool, error)
	recordGetCallback func(objectClass, key string, depth int, handleTransaction bool) (map[string]interface{}, error)
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
						return &types.GenericInnerLink{ObjectName: objectMeta.Name, Pk: pkValue, PkName: objectMeta.Key.Name}, nil
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
	if objectMeta, _, err := validator.metaGetCallback(objectName, true); err != nil {
		return nil, errors.NewDataError(fieldDescription.Meta.Name, errors.ErrWrongFiledType, "Object '%s' referenced in '%s'`s value does not exist", fieldDescription.Name)
	} else {
		return objectMeta, nil
	}
}

func (validator *GenericInnerFieldValidator) validateRecordPk(pkValue interface{}, fieldDescription *meta.FieldDescription) (string, error) {
	var validatedPkValue string
	switch castPkValue := pkValue.(type) {
	case float64:
		validatedPkValue = strconv.Itoa(int(castPkValue))
	case int:
		validatedPkValue = strconv.Itoa(castPkValue)
	case string:
		validatedPkValue = castPkValue
	default:
		return "", errors.NewDataError(fieldDescription.Meta.Name, errors.ErrWrongFiledType, "PK value referenced in '%s'`s has wrong type", fieldDescription.Name)
	}
	return validatedPkValue, nil
}

func (validator *GenericInnerFieldValidator) validateRecord(objectMeta *meta.Meta, pkValue string, fieldDescription *meta.FieldDescription) (error) {
	if _, err := validator.recordGetCallback(objectMeta.Name, pkValue, 1, false); err != nil {
		return errors.NewDataError(fieldDescription.Meta.Name, errors.ErrWrongFiledType, "Record of object '%s' with PK '%s' referenced in '%s'`s value does not exist", objectMeta.Name, pkValue, fieldDescription.Name)
	} else {
		return nil
	}
}

func NewGenericInnerFieldValidator(metaGetCallback func(name string, handleTransaction bool) (*meta.Meta, bool, error), recordGetCallback func(objectClass, key string, depth int, handleTransaction bool) (map[string]interface{}, error)) *GenericInnerFieldValidator {
	return &GenericInnerFieldValidator{metaGetCallback: metaGetCallback, recordGetCallback: recordGetCallback}
}
