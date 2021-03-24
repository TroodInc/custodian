package migrations

type MigrationErrorCode string

const (
	MigrationErrorNotImplemented              = "not_implemented"
	MigrationErrorDuplicated                  = "duplicated_error"
	MigrationErrorInvalidDescription          = "invalid_description"
	MigrationErrorAlreadyHasBeenApplied       = "migration_already_applied"
	MigrationNoChangesWereDetected            = "no_changes_were_detected"
	MigrationErrorPreviousStateFieldNotFound  = "previous_state_field_not_found"
	MigrationErrorPreviousStateActionNotFound = "previous_state_action_not_found"
)
