package description

import (
	"custodian/utils"
	"fmt"
)

type ValidationError struct {
	Message string
}

func (err *ValidationError) Error() string {
	return err.Message
}

type MetaValidationService struct {
}

func (validationService *MetaValidationService) Validate(metaDescription *MetaDescription) (bool, error) {
	if ok, err := validationService.checkFieldsDoesNotContainDuplicates(metaDescription.Fields); !ok {
		return false, err
	}
	return true, nil
}

//check if meta contains fields with duplicated name
func (validationService *MetaValidationService) checkFieldsDoesNotContainDuplicates(fields []Field) (bool, error) {
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
