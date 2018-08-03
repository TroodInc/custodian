package errors

import (
	"encoding/json"
	"fmt"
)

type DataError struct {
	Code        string
	Msg         string
	objectClass string
}

func (e *DataError) Error() string {
	return fmt.Sprintf("Data error:  object class = '%s', code='%s'  msg = '%s'", e.objectClass, e.Code, e.Msg)
}

func (e *DataError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"objectClass": e.objectClass,
		"code":        "table:" + e.Code,
		"msg":         e.Msg,
	})
	return j
}

func NewDataError(objectClass string, code string, msg string, a ...interface{}) *DataError {
	return &DataError{objectClass: objectClass, Code: code, Msg: fmt.Sprintf(msg, a...)}
}
