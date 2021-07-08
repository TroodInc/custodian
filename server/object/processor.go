package object

import (
	"bytes"
	"custodian/server/transactions"
	"database/sql"
	"fmt"
	"github.com/jackc/pgconn"
	"regexp"

	_ "github.com/jackc/pgx/v4/stdlib"
	// _ "github.com/lib/pq"
	"custodian/logger"
	"custodian/server/errors"
	"custodian/server/object/description"
	"custodian/server/object/dml_info"

	"custodian/utils"
	"strconv"
	"strings"
	"text/template"
)

type DBManager struct {}

const (
	ErrTemplateFailed     = "template_failed"
	ErrInvalidArgument    = "invalid_argument"
	ErrDMLFailed          = "dml_failed"
	ErrValidation         = "validation_error"
	ErrValueDuplication   = "duplicated_value_error"
	ErrConvertationFailed = "convertation_failed"
	ErrCommitFailed       = "commit_failed"
)

//{{ if isLast $key .Cols}}{{else}},{{end}}
const (
	templInsert      = `INSERT INTO {{.Table}} {{if not .Cols}} DEFAULT VALUES {{end}}  {{if .Cols}} ({{join .Cols ", "}}) VALUES {{.GetValues}} {{end}} {{if .RCols}} RETURNING {{join .RCols ", "}}{{end}};`
	templFixSequence = `SELECT setval('{{.Table}}_{{.Field}}_seq',(SELECT CAST(MAX("{{.Field}}") AS INT) FROM {{.Table}}), true);`
	templSelect      = `SELECT {{join .Cols ", "}} FROM {{.From}}{{if .Where}} WHERE {{.Where}}{{end}}{{if .Order}} ORDER BY {{.Order}}{{end}}{{if .Limit}} LIMIT {{.Limit}}{{end}}{{if .Offset}} OFFSET {{.Offset}}{{end}}`
	templDelete      = `DELETE FROM {{.Table}}{{if .Filters}} WHERE {{join .Filters " AND "}}{{end}}`
	templUpdate      = `UPDATE {{.Table}} SET {{join .Values ","}}{{if .Filters}} WHERE {{join .Filters " AND "}}{{end}}{{if .Cols}} RETURNING {{join .Cols ", "}}{{end}}`
)

var funcs = template.FuncMap{"join": strings.Join}
var parsedTemplFixSequense = template.Must(template.New("dml_fixsequense").Funcs(funcs).Parse(templFixSequence))
var parsedTemplInsert = template.Must(template.New("dml_insert").Funcs(funcs).Parse(templInsert))
var parsedTemplSelect = template.Must(template.New("dml_select").Funcs(funcs).Parse(templSelect))
var parsedTemplDelete = template.Must(template.New("dml_delete").Funcs(funcs).Parse(templDelete))
var parsedTemplUpdate = template.Must(template.New("dml_update").Funcs(funcs).Parse(templUpdate))

//TODO: move SelectInfo to dml_info and implement constructor method NewSelectInfo with escaping
type SelectInfo struct {
	Cols   []string
	From   string
	Where  string
	Order  string
	Limit  string
	Offset string
}

func (selectInfo *SelectInfo) sql(sql *bytes.Buffer) error {
	return parsedTemplSelect.Execute(sql, selectInfo)
}

func NewSelectInfo(objectMeta *Meta, fields []*FieldDescription, filterKeys []string) *SelectInfo {
	whereExpression := ""
	for i, key := range filterKeys {
		if i > 0 {
			whereExpression += " AND "
		}
		whereExpression += dml_info.EscapeColumn(key) + "=$" + strconv.Itoa(i+1)
	}
	return &SelectInfo{From: GetTableName(objectMeta.Name), Cols: dml_info.EscapeColumns(fieldsToCols(fields, "")), Where: whereExpression}
}

func getFieldsColumnsNames(fields []*FieldDescription) []string {
	names := make([]string, 0)
	for _, field := range fields {
		switch field.Type {
		case description.FieldTypeGeneric:
			names = append(names, GetGenericFieldTypeColumnName(field.Name))
			names = append(names, GetGenericFieldKeyColumnName(field.Name))
		default:
			names = append(names, field.Name)
		}
	}
	return names
}

func newFieldValue(f *FieldDescription, isOptional bool) (interface{}, error) {
	switch f.Type {
	case description.FieldTypeString, description.FieldTypeDate, description.FieldTypeDateTime, description.FieldTypeTime, description.FieldTypeEnum:
		if isOptional {
			return new(sql.NullString), nil
		} else {
			return new(string), nil
		}
	case description.FieldTypeNumber:
		if isOptional {
			return new(sql.NullFloat64), nil
		} else {
			return new(float64), nil
		}
	case description.FieldTypeBool:
		if isOptional {
			return new(sql.NullBool), nil
		} else {
			return new(bool), nil
		}
	case description.FieldTypeObject, description.FieldTypeArray:
		if f.LinkType == description.LinkTypeInner {
			return newFieldValue(f.LinkMeta.Key, f.Optional)
		} else {
			return newFieldValue(f.OuterLinkField, f.Optional)
		}
	case description.FieldTypeGeneric:
		if f.LinkType == description.LinkTypeInner {
			return []interface{}{new(sql.NullString), new(sql.NullString)}, nil
		} else {
			return newFieldValue(f.OuterLinkField, f.Optional)
		}
	default:
		return nil, errors.NewFatalError(ErrConvertationFailed, "Unknown field type '%s'", f.Type)
	}
}

type Stmt struct {
	*sql.Stmt
}

type StmtPreparer interface {
	Prepare(string) (*sql.Stmt, error)
}

func NewStmt(tx *sql.Tx, q string) (*Stmt, error) {
	statement, err := tx.Prepare(q)
	if err != nil {
		logger.Error("Prepare statement error: %s %s", q, err.Error())
		return nil, errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
	}
	logger.Debug("Prepared sql: %s", q)
	return &Stmt{statement}, nil
}

func (s *Stmt) ParsedQuery(binds []interface{}, fields []*FieldDescription) ([]map[string]interface{}, error) {
	rows, err := s.Query(binds)
	if err != nil {
		logger.Error("Query statement error: %s", err.Error())
		return nil, err
	}
	defer rows.Close()
	return rows.Parse(fields)
}

func (s *Stmt) ParsedSingleQuery(binds []interface{}, fields []*FieldDescription) (map[string]interface{}, error) {
	objs, err := s.ParsedQuery(binds, fields)
	if err != nil {
		return nil, err
	}

	count := len(objs)
	if count == 0 {
		return nil, errors.NewFatalError(ErrNotFound, "No rows returned. Check the object's references, identificator and cas value.", nil)
	} else if count > 1 {
		return nil, errors.NewFatalError(ErrTooManyFound, "More then one rows returned", nil)
	}

	return objs[0], nil

}

func (s *Stmt) Query(binds []interface{}) (*Rows, error) {
	rows, err := s.Stmt.Query(binds...)
	if err != nil {
		logger.Error("Execution statement error: %s\nBinds: %s", err.Error(), binds)
		if err, ok := err.(*pgconn.PgError); ok {
			switch err.Code {
			case "23502",
				"23503":
				return nil, errors.NewValidationError(ErrValidation, err.Error(), nil) // Return data here
			case "23505":
				re := regexp.MustCompile(`\(([^)]+)\)=\(([^)]+)\)`)
				parts := re.FindStringSubmatch(err.Detail)[1:]
				data := make(map[string]interface{})
				data[parts[0]] = parts[1]

				return nil, errors.NewValidationError(ErrValueDuplication, err.Error(), data) // Return data here
			default:
				return nil, errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
			}
		} else {
			return nil, errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
		}
	}

	return &Rows{rows}, nil
}

func (s *Stmt) Scalar(receiver interface{}, binds []interface{}) error {
	if rows, err := s.Query(binds); err != nil {
		return err
	} else {
		defer rows.Close()
		rows.Next()
		if err := rows.Scan(receiver); err != nil {
			return err
		}
	}
	return nil
}

func updateNodes(nodes map[string]interface{}, dbObj map[string]interface{}) {
	for fieldName, rv := range dbObj {
		if val, ok := nodes[fieldName]; ok {
			switch val := val.(type) {
			case LazyLink:
				continue
			case DLink:
				val.Id = rv
			case *GenericInnerLink:
				if val.Pk != nil {
					nodes[fieldName] = map[string]interface{}{val.PkName: val.Pk, GenericInnerLinkObjectKey: val.ObjectName}
				} else {
					nodes[fieldName] = nil
				}
			default:
				nodes[fieldName] = rv
			}
		} else {
			nodes[fieldName] = rv
		}
	}
}

func getColumnsToInsert(fieldNames []string, rawValues []interface{}) []string {
	columns := make([]string, 0, 0)
	for i, fieldName := range fieldNames {
		switch rawValues[i].(type) {
		case *GenericInnerLink:
			columns = append(columns, GetGenericFieldTypeColumnName(fieldName))
			columns = append(columns, GetGenericFieldKeyColumnName(fieldName))
		default:
			columns = append(columns, fieldName)
		}
	}
	return columns
}

func getValuesToInsert(fieldNames []string, rawValues map[string]interface{}, expectedSetOfColumns []string) ([]interface{}, error) {
	values := make([]interface{}, 0)
	processedColumns := make([]string, 0)
	for _, fieldName := range fieldNames {
		switch castValue := rawValues[fieldName].(type) {
		case *GenericInnerLink:
			values = append(values, castValue.ObjectName)
			castValuePk := fmt.Sprintf("%v", castValue.Pk)
			values = append(values, castValuePk)
			processedColumns = append(processedColumns, GetGenericFieldTypeColumnName(fieldName))
			processedColumns = append(processedColumns, GetGenericFieldKeyColumnName(fieldName))
		case LazyLink:
			values = append(values, castValue.Obj[castValue.Field.Meta.Key.Name])
			processedColumns = append(processedColumns, fieldName)
		case DLink:
			values = append(values, castValue.Id)
			processedColumns = append(processedColumns, fieldName)
		default:
			switch castValue.(type) {
			case nil:
				values = append(values, castValue)
			default:
				values = append(values, fmt.Sprintf("%v", castValue))
			}
			processedColumns = append(processedColumns, fieldName)
		}
	}
	if !utils.Equal(expectedSetOfColumns, processedColumns, false) {
		return nil, errors.NewValidationError(ErrInvalidArgument, "FieldDescription '%s' not found. All objects must have the same set of fileds.", nil)
	}
	return values, nil
}

func emptyOperation(dbTransaction transactions.DbTransaction) error {
	return nil
}

func alinkVal(v interface{}) interface{} {
	al := v.(LazyLink)
	return al.Obj[al.Field.Meta.Key.Name]
}

func dlinkVal(v interface{}) interface{} {
	return v.(DLink).Id
}

func genericInnerLinkValue(value interface{}) interface{} {
	castValue := value.(*GenericInnerLink)
	if castValue.FieldDescription != nil {
		pkValueAsString, _ := castValue.FieldDescription.ValueAsString(castValue.Pk)
		return []string{castValue.ObjectName, pkValueAsString}
	} else {
		return []interface{}{nil, nil}
	}
}

func identityVal(v interface{}) interface{} {
	return v
}

func increaseCasVal(v interface{}) interface{} {
	cas := v.(float64)
	return cas + 1
}

func fieldsToCols(fields []*FieldDescription, alias string) []string {
	columns := getFieldsColumnsNames(fields)
	for i, column := range columns {
		if alias != "" {
			column = fmt.Sprintf("%s.\"%s\"", alias, column)
		}
		columns[i] = column
	}
	return columns
}

// GetMapKeysStrValues added to adjust pgx driver
// it is necessery to add binds in string type or in nil type.
func GetMapKeysStrValues(targetMap map[string]interface{}) ([]string, []interface{}) {
	values := make([]interface{}, 0)
	keys := make([]string, 0)
	for key, value := range targetMap {
		switch value.(type) {
		case nil:
			values = append(values, nil)
		default:
			values = append(values, fmt.Sprintf("%v", value))
		}

		keys = append(keys, key)
	}
	return keys, values
}
