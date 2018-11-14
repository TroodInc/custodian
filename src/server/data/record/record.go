package record

import (
	"server/object/meta"
	"server/data/types"
	"server/object/description"
)

type Record struct {
	Meta    *meta.Meta
	Data    map[string]interface{}
	RawData map[string]interface{}
}

//replace links with their records` values
func (record *Record) CollapseLinks() {
	for k, v := range record.Data {
		switch link := v.(type) {
		case types.ALink:
			if link.IsOuter {
				if link.Field.Type == description.FieldTypeArray {
					if a, prs := link.Obj[link.Field.Name]; !prs || a == nil {
						link.Obj[link.Field.Name] = make([]interface{}, link.NeighboursCount)
					}
					(link.Obj[link.Field.Name]).([]interface{})[link.Index] = record.Data
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
		case *types.AGenericInnerLink:
			if link.LinkType == description.LinkTypeOuter {
				if _, ok := record.Data[link.Field.Name]; !ok {
					link.RecordData[link.Field.Name] = make([]interface{}, link.NeighboursCount)
				}
				link.RecordData[link.Field.Name].([]interface{})[link.Index] = link.GenericInnerLink.Pk
			} else {
				link.RecordData[link.Field.Name] = link.GenericInnerLink.AsMap()
			}
		}
	}
}

//fill data with values RawData, which contains actual data after DB operations
func (record *Record) MergeData() {
	for k := range record.RawData {
		if value, ok := record.Data[k]; ok {
			if _, ok := value.(types.ALink); ok {
				continue
			}
			if _, ok := value.(*types.AGenericInnerLink); ok {
				continue
			}
		}
		record.Data[k] = record.RawData[k]
	}
}

//prepare data for DB operations: replace complex links with their primitive values
func (record *Record) PrepareData() {
	record.RawData = map[string]interface{}{}
	for k, v := range record.Data {
		switch link := v.(type) {
		case types.ALink:
			if link.IsOuter {
				record.RawData[k] = link.Obj[link.Field.LinkMeta.Key.Name]
			} else {
				record.RawData[k] = link.Obj[link.Field.Meta.Key.Name]
			}
		case *types.AGenericInnerLink:
			//fill PK if it is presented in RecordData stash, case of newly created record
			if pkValue, ok := link.RecordData[link.GenericInnerLink.PkName]; ok {
				link.GenericInnerLink.Pk = pkValue
			}
			record.RawData[k] = link.GenericInnerLink
		default:
			record.RawData[k] = record.Data[k]
		}
	}
}

func (record *Record) Pk() interface{} {
	return record.Data[record.Meta.Key.Name]
}

func (record *Record) PkAsString() string {
	pkAsString, _ := record.Meta.Key.ValueAsString(record.Pk())
	return pkAsString
}

func (record *Record) IsPhantom() bool {
	// probably not best solution to determine isPhantom value, but it requires DB access for each record otherwise
	_, pkIsSet := record.Data[record.Meta.Key.Name]
	return !pkIsSet
}

func NewRecord(meta *meta.Meta, data map[string]interface{}) *Record {
	return &Record{Meta: meta, Data: data, RawData: nil}
}
