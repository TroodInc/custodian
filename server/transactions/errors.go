package transactions

import (
	"fmt"

	"github.com/getsentry/sentry-go"
)

type TransactionError struct {
	Msg string
}

func (e *TransactionError) Error() string {
	sentry.CaptureMessage(e.Msg)
	return fmt.Sprintf("Transaction error: %s", e.Msg)
}
