package record

import (
	"server/object/meta"
	"server/data/types"
	"server/object/description"
)

type Record struct {
	Meta      *meta.Meta
	Data      map[string]interface{}
	IsPhantom bool
}

//replace links with their records` values
func (record *Record) CollapseLinks() {
	for k, v := range record.Data {
		switch link := v.(type) {
		case types.ALink:
			if link.IsOuter {
				if link.Field.Type == description.FieldTypeArray {
					if a, prs := link.Obj[link.Field.Name]; !prs || a == nil {
						link.Obj[link.Field.Name] = []interface{}{record.Data}
					} else {
						link.Obj[link.Field.Name] = append(a.([]interface{}), record.Data)
					}
				} else if link.Field.Type == description.FieldTypeObject {
					link.Obj[link.Field.Name] = record.Data
				}
				record.Data[k] = link.Obj[link.Field.LinkMeta.Key.Name]
			} else {
				record.Data[k] = link.Obj
			}
		case types.DLink:
			if !link.IsOuter {
				record.Data[k] = link.Id
			}
		}
	}
}

//replace links with their PK values
func (record *Record) NormalizeLinks() {
	for k, v := range record.Data {
		switch link := v.(type) {
		case types.ALink:
			if link.IsOuter {
				if link.Field.Type == description.FieldTypeArray {
					if a, prs := link.Obj[link.Field.Name]; !prs || a == nil {
						link.Obj[link.Field.Name] = []interface{}{record.Data}
					} else {
						link.Obj[link.Field.Name] = append(a.([]interface{}), record.Data)
					}
				} else if link.Field.Type == description.FieldTypeObject {
					link.Obj[link.Field.Name] = record.Data
				}
				record.Data[k] = link.Obj[link.Field.LinkMeta.Key.Name]
			} else {
				record.Data[k] = link.Obj[link.Field.Meta.Key.Name]
			}
		case types.DLink:
			if !link.IsOuter {
				record.Data[k] = link.Id
			}
		}
	}
}

func (record *Record) Pk() interface{} {
	return record.Data[record.Meta.Key.Name]
}

func NewRecord(meta *meta.Meta, data map[string]interface{}) *Record {
	_, pkIsSet := data[meta.Key.Name]
	// probably not best solution to determine isPhantom value, but it requires DB access for each record otherwise
	return &Record{Meta: meta, Data: data, IsPhantom: !pkIsSet || !meta.Key.Field.Optional}
}
