package description

import (
	"utils"
	"fmt"
	"server/errors"
)

//type ValidationError struct {
//	Message string
//}
//
//func (err *ValidationError) Error() string {
//	return err.Message
//}
//
//type ValidationService struct {
//}
//
//func (validationService *ValidationService) Validate(metaDescription *MetaDescription) (bool, error) {
//	if ok, err := validationService.checkFieldsDoesNotContainDuplicates(metaDescription.Fields); !ok {
//		return false, err
//	}
//	return true, nil
//}

//check if meta contains fields with duplicated name
func CheckFieldsDoesNotContainDuplicates(fields []Field) (bool, error) {
	fieldNames := make([]string, 0)
	for _, field := range fields {
		if !utils.Contains(fieldNames, field.Name) {
			fieldNames = append(fieldNames, field.Name)
		} else {
			// TODO: Add error code
			return false, errors.NewValidationError("", fmt.Sprintf("Object contains duplicated field '%s'", field.Name), nil)
		}
	}
	return true, nil
}
