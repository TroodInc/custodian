package transactions

import (
	"encoding/json"
	"fmt"
)

const (
	ErrCommitFailed = "commit_failed"
)

type TransactionError struct {
	code string
	msg  string
}

func (e *TransactionError) Error() string {
	return fmt.Sprintf("Transaction error:  code='%s'  msg = '%s'", e.code, e.msg)
}

func (e *TransactionError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"code": "dml:" + e.code,
		"msg":  e.msg,
	})
	return j
}

func NewTransactionError(code string, msg string, a ...interface{}) *TransactionError {
	return &TransactionError{code: code, msg: fmt.Sprintf(msg, a...)}
}
