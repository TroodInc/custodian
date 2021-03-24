package description

import (
	"fmt"
	"encoding/json"
)

type metaDescriptionError struct {
	code string
	msg  string
	meta string
	op   string
}

func (e *metaDescriptionError) Error() string {
	return fmt.Sprintf("MetaDescription error:  MetaDescription = '%s', operation = '%s', code='%s'  msg = '%s'", e.meta, e.op, e.code, e.msg)
}

func (e *metaDescriptionError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"MetaDescription": e.meta,
		"op":              e.op,
		"code":            "MetaDescription:" + e.code,
		"msg":             e.msg,
	})
	return j
}

func NewMetaDescriptionError(meta string, op string, code string, msg string, a ...interface{}) *metaDescriptionError {
	return &metaDescriptionError{meta: meta, op: op, code: code, msg: fmt.Sprintf(msg, a...)}
}

const (
	ErrNotValid      = "not_valid"
	ErrJsonUnmarshal = "json_unmarshal"
	ErrJsonMarshal   = "json_marshal"
)
