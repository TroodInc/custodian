package pg

import (
	"encoding/json"
	"fmt"
)

// Meta DDL errors
const (
	ErrUnsupportedColumnType = "unsuported_column_type"
	ErrUnsupportedLinkType   = "unsuported_link_type"
	ErrNotFound              = "not_found"
	ErrTooManyFound          = "too_many_found"
	ErrInternal              = "internal"
	ErrWrongDefultValue      = "wrong_default_value"
	ErrExecutingDDL          = "error_exec_ddl"
)

type DDLError struct {
	code  string
	msg   string
	table string
}

func (e *DDLError) Error() string {
	return fmt.Sprintf("DDL error:  table = '%s', code='%s'  msg = '%s'", e.table, e.code, e.msg)
}

func (e *DDLError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"table": e.table,
		"code":  "table:" + e.code,
		"msg":   e.msg,
	})
	return j
}

func (e *DDLError) Code() string {
	return e.code
}

func NewDdlError(code string, msg string, table string) *DDLError {
	return &DDLError{code: code, msg: msg, table: table}
}

type TransactionNotBegunError struct {
	msg string
}

func (err *TransactionNotBegunError) Error() string {
	return "Attempt to execute DB statement within not started transaction"
}
