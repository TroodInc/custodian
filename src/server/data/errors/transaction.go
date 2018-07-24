package errors

type TransactionNotBegunError struct {
	msg string
}

func (err *TransactionNotBegunError) Error() string {
	return "Attempt to execute DB statement within not started transaction"
}
