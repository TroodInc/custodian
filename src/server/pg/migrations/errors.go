package migrations

import (
	"fmt"
)

type PgMigrationError struct {
	message string
}

func (e *PgMigrationError) Error() string {
	return fmt.Sprintf("Database migration appliance error: '%s'", e.message)
}

func NewPgMigrationError(message string) *PgMigrationError {
	return &PgMigrationError{message: message}
}
