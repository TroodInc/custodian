package meta

import (
	"encoding/json"
	"fmt"
)

type metaError struct {
	code string
	msg  string
	meta string
	op   string
}

func (e *metaError) Error() string {
	return fmt.Sprintf("Meta error:  MetaDescription = '%s', operation = '%s', code='%s'  msg = '%s'", e.meta, e.op, e.code, e.msg)
}

func (e *metaError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"MetaDescription": e.meta,
		"op":              e.op,
		"code":            "MetaDescription:" + e.code,
		"msg":             e.msg,
	})
	return j
}

func NewMetaError(meta string, op string, code string, msg string, a ...interface{}) *metaError {
	return &metaError{meta: meta, op: op, code: code, msg: fmt.Sprintf(msg, a...)}
}

const (
	ErrDuplicated    = "duplicated"
	ErrNotFound      = "not_found"
	ErrNotValid      = "not_valid"
	ErrInternal      = "internal"
	ErrJsonUnmarshal = "json_unmarshal"
	ErrJsonMarshal   = "json_marshal"
)
