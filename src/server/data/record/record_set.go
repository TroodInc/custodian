package record

import "server/meta"

type RecordSet struct {
	Meta    *meta.Meta
	DataSet []map[string]interface{}
}
