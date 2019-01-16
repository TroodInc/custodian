package migrations

import (
	"fmt"
)

type MigrationError struct {
	message string
}

func (e *MigrationError) Error() string {
	return fmt.Sprintf("Migration error: '%s'", e.message)
}

func (e *MigrationError) Serialize() map[string]string {
	return map[string]string{"Code": "migration_already_applied", "Message": e.Error()}
}

func NewMigrationError(message string) *MigrationError {
	return &MigrationError{message: message}
}
