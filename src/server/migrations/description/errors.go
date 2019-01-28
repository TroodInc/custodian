package description

import (
	"fmt"
)

type MigrationMarshallingError struct {
	message string
}

func (e *MigrationMarshallingError) Error() string {
	return fmt.Sprintf("Migration marshaling error: '%s'", e.message)
}

func NewMigrationUnmarshallingError(message string) *MigrationMarshallingError {
	return &MigrationMarshallingError{message: message}
}
