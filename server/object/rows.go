package object

import (
	"custodian/server/data/types"
	"custodian/server/errors"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"database/sql"
	"reflect"
	"time"
)

type Rows struct {
	*sql.Rows
}

func (rows *Rows) getDefaultValues(fields []*meta.FieldDescription) ([]interface{}, error) {
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

// parseTime parses three time formats
// 1) "15:04:05.000000-07" which is saved when default function is set to now()
// 2) "15:04:05-07" wich is saved when timezone is in -07 format
// 3) "15:04:05-07:00" wich is saved when timezone is in -07:00 format
// order matters
// TODO set standard time format
func parseTime(t string) (string, error) {
	t1, e := time.Parse("15:04:05.000000-07", t)
	if e == nil {
		return t1.UTC().Format("15:04:05.000000Z"), e
	}
	t2, e2 := time.Parse("15:04:05-07", t)
	if e2 == nil {
		return t2.Format("15:04:05-07:00"), e2
	}
	t3, e3 := time.Parse("15:04:05-07:00", t)
	if e3 == nil {
		return t3.Format("15:04:05-07:00"), e3
	}

	return "", e
}

func parseDate(t string) (string, error) {
	t1, e := time.Parse(time.RFC3339Nano, t)
	if e == nil {
		return t1.UTC().Format("2006-01-02"), e
	}
	return "", e
}

func parseDateTime(t string) (string, error) {
	t1, e := time.Parse(time.RFC3339Nano, t)
	if e == nil {
		return t1.UTC().Format(time.RFC3339Nano), e
	}
	return "", e
}

func (rows *Rows) Parse(fields []*meta.FieldDescription) ([]map[string]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
	}

	result := make([]map[string]interface{}, 0)
	i := 0
	fieldByColumnName := func(columnName string) *meta.FieldDescription {
		fieldName := columnName
		if meta.IsGenericFieldColumn(columnName) {
			fieldName = meta.ReverseGenericFieldName(fieldName)
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
				return nil, errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
			}
			result = append(result, make(map[string]interface{}))
			for j, columnName := range cols {
				if fieldByColumnName(columnName).Type == description.FieldTypeDate {
					switch value := values[j].(type) {
					case *sql.NullString:
						if value.Valid {
							t, err := parseDate(value.String)
							if err != nil {
								return nil, errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
							}
							result[i][columnName] = t
						} else {
							result[i][columnName] = nil
						}
					case *string:
						t, err := parseDate(*value)
						if err != nil {
							return nil, errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
						}
						result[i][columnName] = t
					}
				} else if fieldByColumnName(columnName).Type == description.FieldTypeTime {
					switch value := values[j].(type) {
					case *sql.NullString:
						if value.Valid {
							t, err := parseTime(value.String)
							if err != nil {
								return nil, errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
							}
							result[i][columnName] = t
						} else {
							result[i][columnName] = nil
						}
					case *string:
						t, err := parseTime(*value)
						if err != nil {
							return nil, errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
						}
						result[i][columnName] = t
					}
				} else if fieldByColumnName(columnName).Type == description.FieldTypeDateTime {
					switch value := values[j].(type) {
					case *sql.NullString:
						if value.Valid {
							t, err := parseDateTime(value.String)
							if err != nil {
								return nil, errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
							}
							result[i][columnName] = t
						} else {
							result[i][columnName] = nil
						}
					case *string:
						t, err := parseDateTime(*value)
						if err != nil {
							return nil, errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
						}
						result[i][columnName] = t
					}
				} else if fieldDescription := fieldByColumnName(columnName); fieldDescription.Type == description.FieldTypeGeneric {

					//
					value := values[j].(*sql.NullString)
					assembledValue, ok := result[i][fieldDescription.Name]
					var castAssembledValue *types.GenericInnerLink
					if !ok || assembledValue == nil {
						// create new otherwise
						castAssembledValue = &types.GenericInnerLink{}
					} else {
						// get already assembled value if it exists
						castAssembledValue = assembledValue.(*types.GenericInnerLink)
					}
					//fill corresponding value
					if meta.IsGenericFieldTypeColumn(columnName) {
						if value.String != "" {
							castAssembledValue.ObjectName = value.String
							if linkMeta := fieldDescription.LinkMetaList.GetByName(value.String); linkMeta == nil {
								return nil, errors.NewFatalError(ErrDMLFailed, "Generic field '%s' references improper meta '%s'", map[string]string{
									"field":  fieldDescription.Name,
									"object": castAssembledValue.ObjectName,
								})
							} else {
								castAssembledValue.PkName = linkMeta.Key.Name
							}
						}
					} else if meta.IsGenericFieldKeyColumn(columnName) {
						if value.String != "" {
							if linkMeta := fieldDescription.LinkMetaList.GetByName(castAssembledValue.ObjectName); linkMeta == nil {
								return nil, errors.NewFatalError(ErrDMLFailed, "Generic field %s references improper meta'%s'", map[string]string{
									"field":  fieldDescription.Name,
									"object": castAssembledValue.ObjectName,
								})
							} else {
								castAssembledValue.Pk, _ = linkMeta.Key.ValueFromString(value.String)
							}
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
						return nil, errors.NewFatalError(ErrDMLFailed, "unknown reference type '%s'", reflect.TypeOf(values[j]).String())
					}
				}
			}
			i++
		}

	}
	return result, nil
}
