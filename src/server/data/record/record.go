package record

import (
	"server/data/types"
	"server/object"
	"time"
)

type Record struct {
	Meta    *object.Meta
	Data    map[string]interface{}
	RawData map[string]interface{}
	Links   []*types.LazyLink
}

func (record *Record) GetData() map[string]interface{} {
	data := make(map[string]interface{})
	for key, val := range record.Data {
		switch val.(type) {
			case *Record:
				data[key] = val.(*Record).GetData()

			case *types.GenericInnerLink:
				data[key] = val.(*types.GenericInnerLink).AsMap()

			case []interface{}:
				list := make([]interface{}, 0)
				for _, item := range val.([]interface{}) {
					switch item.(type) {
						case *Record:
							list = append(list, item.(*Record).GetData())
						case *types.GenericInnerLink:
							list = append(list, item.(*types.GenericInnerLink).AsMap())
						default:
							list = append(list, item)
					}
				}
				data[key] = list

			default:
				data[key] = val
		}
	}

	return data
}

//replace links with their records` values
func (record *Record) CollapseLinks() {
	//TODO: all Links should be placed into Record.Links, links in Record`s data
	for k, v := range record.Data {
		switch link := v.(type) {
		case types.LazyLink:
			if link.IsOuter {
				if link.Field.Type == object.FieldTypeArray {
					if a, prs := link.Obj[link.Field.Name]; !prs || a == nil {
						link.Obj[link.Field.Name] = make([]interface{}, link.NeighboursCount)
					}
					(link.Obj[link.Field.Name]).([]interface{})[link.Index] = record.Data
				} else if link.Field.Type == object.FieldTypeObjects {
					if a, prs := link.Obj[link.Field.Name]; !prs || a == nil {
						link.Obj[link.Field.Name] = make([]interface{}, link.NeighboursCount)
					}
					(link.Obj[link.Field.Name]).([]interface{})[link.Index] = record.Data
				} else if link.Field.Type == object.FieldTypeObject {
					link.Obj[link.Field.Name] = record.Data
				}
				record.Data[k] = link.Obj[link.Field.LinkMeta.Key]
			} else {
				record.Data[k] = link.Obj
			}
		case types.DLink:
			if !link.IsOuter {
				record.Data[k] = link.Id
			}
		case *types.AGenericInnerLink:
			if link.LinkType == object.LinkTypeOuter {
				if _, ok := record.Data[link.Field.Name]; !ok {
					link.RecordData[link.Field.Name] = make([]interface{}, link.NeighboursCount)
				}
				link.RecordData[link.Field.Name].([]interface{})[link.Index] = link.GenericInnerLink.Pk
			} else {
				link.RecordData[link.Field.Name] = link.GenericInnerLink.AsMap()
			}
		}
	}
	//
	for _, link := range record.Links {
		//cases:
		//1. Link is a reference to a record, which owns this one within "Objects" field
		if link.IsOuter {
			if a, prs := link.Obj[link.Field.Name]; !prs || a == nil {
				link.Obj[link.Field.Name] = make([]interface{}, link.NeighboursCount)
			}
			(link.Obj[link.Field.Name]).([]interface{})[link.Index] = record.Data
		}
	}
}

//fill data with values RawData, which contains actual data after DB operations
func (record *Record) MergeData() {
	for k := range record.RawData {
		if value, ok := record.Data[k]; ok {
			if _, ok := value.(types.LazyLink); ok {
				continue
			}
			if _, ok := value.(*types.AGenericInnerLink); ok {
				continue
			}
		}
		record.Data[k] = record.RawData[k]
	}
}

// TODO: Must be removed
func (record *Record) GetDataForNotification() map[string]interface{} {
	adaptedRecordData := map[string]interface{}{}
	for key, value := range record.Data {
		switch castValue := value.(type) {
		case types.DLink:
			adaptedRecordData[key] = castValue.Id
		default:
			adaptedRecordData[key] = castValue
		}
	}
	return adaptedRecordData
}

//prepare data for DB operations: replace complex links with their primitive values,
func (record *Record) PrepareData(operationType RecordOperationType) {
	record.RawData = map[string]interface{}{}
	for k, v := range record.Data {
		switch link := v.(type) {
		case types.LazyLink:
			if link.IsOuter {
				record.RawData[k] = link.Obj[link.Field.LinkMeta.Key]
			} else {
				record.RawData[k] = link.Obj[link.Field.Meta.Key]
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
	record.setAutoValues(operationType)
}

func (record *Record) setAutoValues(operationType RecordOperationType) {
	for _, field := range record.Meta.Fields {
		if operationType == RecordOperationTypeUpdate && field.NowOnUpdate || operationType == RecordOperationTypeCreate && field.NowOnCreate {
			var value string
			switch field.Type {
			case object.FieldTypeDateTime:
				value = time.Now().UTC().Format("2006-01-02T15:04:05.123456789Z07:00")
			case object.FieldTypeDate:
				value = time.Now().UTC().Format("2006-01-02")
			case object.FieldTypeTime:
				value = time.Now().UTC().Format("15:04:05.123456789")
			}
			if value != "" {
				record.RawData[field.Name] = value
			}
		}
	}
}

func (record *Record) Pk() interface{} {
	return record.Data[record.Meta.Key]
}

func (record *Record) PkAsString() string {
	keyField := record.Meta.FindField(record.Meta.Key)
	pkAsString, _ := keyField.ValueAsString(record.Pk())
	return pkAsString
}

func (record *Record) IsPhantom() bool {
	// probably not best solution to determine isPhantom value, but it requires DB access for each record otherwise
	_, pkIsSet := record.Data[record.Meta.Key]
	return !pkIsSet
}

func NewRecord(meta *object.Meta, data map[string]interface{}) *Record {
	return  &Record{Meta: meta, Data: data, RawData: nil, Links: make([]*types.LazyLink, 0)}
}
