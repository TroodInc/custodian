package migrations

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
