package transactions

import "fmt"

type TransactionError struct {
	msg string
}

func (e *TransactionError) Error() string {
	return fmt.Sprintf("Transaction error: %s", e.msg)
}
