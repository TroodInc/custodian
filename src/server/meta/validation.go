package meta

import (
	"utils"
	"fmt"
)

type ValidationError struct {
	Message string
}

func (err *ValidationError) Error() string {
	return err.Message
}

type ValidationService struct {
}

func (validationService *ValidationService) Validate(metaDescription *MetaDescription) (bool, error) {
	if ok, err := validationService.checkFieldsDoesNotContainDuplicates(metaDescription.Fields); !ok {
		return false, err
	}
	for _, field := range metaDescription.Fields {
		if ok, err := validationService.checkFieldIsConsistent(field); !ok {
			return false, err
		}
	}
	return true, nil

}

//check if meta contains fields with duplicated name
func (validationService *ValidationService) checkFieldsDoesNotContainDuplicates(fields []Field) (bool, error) {
	fieldNames := make([]string, 0)
	for _, field := range fields {
		if !utils.Contains(fieldNames, field.Name) {
			fieldNames = append(fieldNames, field.Name)
		} else {
			return false, &ValidationError{fmt.Sprintf("Object contains duplicated field '%s'", field.Name)}
		}
	}
	return true, nil
}

func (validationService *ValidationService) checkFieldIsConsistent(field Field) (bool, error) {
	if !field.Optional && field.Def != nil {
		return false, &ValidationError{fmt.Sprintf("Mandatory field '%s' cannot have default value", field.Name)}
	}
	return true, nil
}
