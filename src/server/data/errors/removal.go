package errors

import (
	"fmt"
)

type RemovalError struct {
	Msg      string
	MetaName string
}

func (e RemovalError) Error() string {
	return fmt.Sprintf("Error accured while attempting to remove record of '%s': %s", e.MetaName, e.Msg)
}

func NewRemovalError(MetaName string, msg string) *RemovalError {
	return &RemovalError{MetaName: MetaName, Msg: msg}
}
