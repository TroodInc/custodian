package pg

import (
	"database/sql"
	"reflect"
	"server/meta"
	"server/data/types"
)

type Rows struct {
	*sql.Rows
}

func (rows *Rows) getDefaultValues(fields []*object.FieldDescription, ) ([]interface{}, error) {
	values := make([]interface{}, 0)
	for _, field := range fields {
		if newValue, err := newFieldValue(field, field.Optional); err != nil {
			return nil, err
		} else {
			if castValues, ok := newValue.([]interface{}); ok {
				values = append(values, castValues...)
			} else {
				values = append(values, newValue)
			}
		}
	}
	return values, nil
}

func (rows *Rows) Parse(fields []*object.FieldDescription) ([]map[string]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, NewDMLError(ErrDMLFailed, err.Error())
	}

	result := make([]map[string]interface{}, 0)
	i := 0
	fieldByColumnName := func(columnName string) *object.FieldDescription {
		fieldName := columnName
		if object.IsGenericFieldColumn(columnName) {
			fieldName = object.ReverseGenericFieldName(fieldName)
		}

		for _, field := range fields {
			if field.Name == fieldName {
				return field
			}
		}
		return nil
	}

	for rows.Next() {
		if values, err := rows.getDefaultValues(fields); err != nil {
			return nil, err
		} else {
			if err = rows.Scan(values...); err != nil {
				return nil, NewDMLError(ErrDMLFailed, err.Error())
			}
			result = append(result, make(map[string]interface{}))
			for j, columnName := range cols {
				if fieldByColumnName(columnName).Type == object.FieldTypeDate {
					switch value := values[j].(type) {
					case *sql.NullString:
						if value.Valid {
							result[i][columnName] = string([]rune(value.String)[0:10])
						} else {
							result[i][columnName] = nil
						}
					case *string:
						result[i][columnName] = string([]rune(*value)[0:10])
					}
				} else if fieldByColumnName(columnName).Type == object.FieldTypeTime {
					switch value := values[j].(type) {
					case *sql.NullString:
						if value.Valid {
							result[i][columnName] = string([]rune(value.String)[11:])
						} else {
							result[i][columnName] = nil
						}
					case *string:
						result[i][columnName] = string([]rune(*value)[11:])
					}
				} else if fieldByColumnName(columnName).Type == object.FieldTypeDateTime {
					switch value := values[j].(type) {
					case *sql.NullString:
						if value.Valid {
							result[i][columnName] = value.String
						} else {
							result[i][columnName] = nil
						}
					case *string:
						result[i][columnName] = *value
					}
				} else if fieldDescription := fieldByColumnName(columnName); fieldDescription.Type == object.FieldTypeGeneric {

					//
					value := values[j].(*sql.NullString)
					assembledValue, ok := result[i][fieldDescription.Name]
					var castAssembledValue types.GenericInnerLink
					if !ok || assembledValue == nil {
						// create new otherwise
						castAssembledValue = types.GenericInnerLink{}
					} else {
						// get already assembled value if it exists
						castAssembledValue = assembledValue.(types.GenericInnerLink)
					}
					//fill corresponding value
					if object.IsGenericFieldTypeColumn(columnName) {
						castAssembledValue.ObjectName = value.String
						if value.String != "" {
							castAssembledValue.PkName = fieldDescription.LinkMetaList.GetByName(value.String).Key.Name
						}
					} else if object.IsGenericFieldKeyColumn(columnName) {
						if value.String != "" {
							castAssembledValue.Pk, _ = fieldDescription.LinkMetaList.GetByName(castAssembledValue.ObjectName).Key.ValueFromString(value.String)
						}
					}
					//include value if it contains not null data
					if castAssembledValue.Pk != nil || castAssembledValue.ObjectName != "" {
						result[i][fieldDescription.Name] = castAssembledValue
					} else {
						result[i][fieldDescription.Name] = nil
					}

				} else {
					switch t := values[j].(type) {
					case *string:
						result[i][columnName] = *t
					case *sql.NullString:
						if t.Valid {
							result[i][columnName] = t.String
						} else {
							result[i][columnName] = nil
						}
					case *float64:
						result[i][columnName] = *t
					case *sql.NullFloat64:
						if t.Valid {
							result[i][columnName] = t.Float64
						} else {
							result[i][columnName] = nil
						}
					case *bool:
						result[i][columnName] = *t
					case *sql.NullBool:
						if t.Valid {
							result[i][columnName] = t.Bool
						} else {
							result[i][columnName] = nil
						}
					default:
						return nil, NewDMLError(ErrDMLFailed, "unknown reference type '%s'", reflect.TypeOf(values[j]).String())
					}
				}
			}
			i++
		}

	}
	return result, nil
}
