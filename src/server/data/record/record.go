package record

import (
	"server/meta"
	"server/data/types"
)

type Record struct {
	Meta *object.Meta
	Data map[string]interface{}
}

func (record *Record) CollapseLinks() {
	collapseLinks(record.Data)
}

func collapseLinks(obj map[string]interface{}) {
	for k, v := range obj {
		switch l := v.(type) {
		case types.ALink:
			if l.IsOuter {
				if l.Field.Type == object.FieldTypeArray {
					if a, prs := l.Obj[l.Field.Name]; !prs || a == nil {
						l.Obj[l.Field.Name] = []interface{}{obj}
					} else {
						l.Obj[l.Field.Name] = append(a.([]interface{}), obj)
					}
				} else if l.Field.Type == object.FieldTypeObject {
					l.Obj[l.Field.Name] = obj
				}
				delete(obj, k)
			} else {
				obj[k] = l.Obj
			}
		case types.DLink:
			if !l.IsOuter {
				obj[k] = l.Id
			}
		}
	}
}
