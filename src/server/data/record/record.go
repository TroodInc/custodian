package record

import "server/meta"

type Record struct {
	Meta *meta.Meta
	Data map[string]interface{}
}
