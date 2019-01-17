package migrations

import (
	"fmt"
)

type MigrationErrorCode string

const (
	MigrationErrorNotImplemented        = "not_implemented"
	MigrationErrorDuplicated            = "duplicated_error"
	MigrationErrorInvalidDescription    = "invalid_description"
	MigrationErrorAlreadyHasBeenApplied = "migration_already_applied"
)

type MigrationError struct {
	message string
	code    MigrationErrorCode
}

func (e *MigrationError) Error() string {
	return fmt.Sprintf("Migration error: '%s'", e.message)
}

func (e *MigrationError) Code() MigrationErrorCode {
	return e.code
}

func (e *MigrationError) Serialize() map[string]interface{} {
	return map[string]interface{}{"Code": e.code, "Message": e.Error()}
}

func NewMigrationError(code MigrationErrorCode, message string) *MigrationError {
	return &MigrationError{code: code, message: message}
}
