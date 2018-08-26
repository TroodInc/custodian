package transactions

import "fmt"

type TransactionError struct {
	Msg string
}

func (e *TransactionError) Error() string {
	return fmt.Sprintf("Transaction error: %s", e.Msg)
}
