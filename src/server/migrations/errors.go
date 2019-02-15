package migrations

import (
	"fmt"
)

type MigrationErrorCode string

const (
	MigrationErrorNotImplemented              = "not_implemented"
	MigrationErrorDuplicated                  = "duplicated_error"
	MigrationErrorInvalidDescription          = "invalid_description"
	MigrationErrorAlreadyHasBeenApplied       = "migration_already_applied"
	MigrationNoChangesWereDetected            = "no_changes_were_detected"
	MigrationErrorWhileWritingMigrationFile   = "error_while_writing_migration_file"
	MigrationErrorParentsChanged              = "migration_parents_have_change"
	MigrationIsNotCompatibleWithSiblings      = "migration_is_not_compatible_with_siblings"
	MigrationIsNotActual                      = "migration_is_not_actual"
	MigrationErrorPreviousStateFieldNotFound  = "previous_state_field_not_found"
	MigrationErrorPreviousStateActionNotFound = "previous_state_action_not_found"
)

type MigrationError struct {
	message string
	code    string
}

func (e *MigrationError) Error() string {
	return fmt.Sprintf("Migration error: '%s'", e.message)
}

func (e *MigrationError) Code() string {
	return e.code
}

func (e *MigrationError) Serialize() map[string]interface{} {
	return map[string]interface{}{"Code": e.code, "Message": e.Error()}
}

func NewMigrationError(code string, message string) *MigrationError {
	return &MigrationError{code: code, message: message}
}
