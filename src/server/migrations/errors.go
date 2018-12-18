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

func NewMigrationError(message string) *MigrationError {
	return &MigrationError{message: message}
}


type MigrationDeserializationError struct {
	message string
}

func (e *MigrationDeserializationError) Error() string {
	return fmt.Sprintf("Migration deserialization error: '%s'", e.message)
}

func NewMigrationDeserializationError(message string) *MigrationDeserializationError {
	return &MigrationDeserializationError{message: message}
}