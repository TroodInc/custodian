package object

import (
	"bytes"
	"custodian/server/transactions"
	"database/sql"
	"fmt"
	"regexp"

	"github.com/jackc/pgconn"

	rqlParser "github.com/Q-CIS-DEV/go-rql-parser"

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

type DBManager struct {
	db *sql.DB
}

func (dm *DBManager) Db() interface{} {
	return dm.db
}

func NewDataManager(db *sql.DB) (*DBManager, error) {
	return &DBManager{db: db}, nil
}

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
	// logger.Debug("Prepared sql: %s", q)
	return &Stmt{statement}, nil
}

func (dm *DBManager) Prepare(q string, tx *sql.Tx) (*Stmt, error) {
	return NewStmt(tx, q)
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

func (dm *DBManager) PrepareUpdateOperation(m *Meta, recordValues []map[string]interface{}) (transactions.Operation, error) {
	if len(recordValues) == 0 {
		return emptyOperation, nil
	}

	rFields := m.TableFields()
	updateInfo := &dml_info.UpdateInfo{GetTableName(m.Name), dml_info.EscapeColumns(getFieldsColumnsNames(rFields)), make([]string, 0), make([]string, 0)}
	updateFields := make([]string, 0, len(recordValues[0]))
	valueExtractors := make([]func(interface{}) interface{}, 0, len(recordValues[0]))
	currentColumnIndex := 0
	var b bytes.Buffer
	newBind := func(col string, columnIndex int) string {
		defer b.Reset()
		b.WriteString(fmt.Sprintf("\"%s\"", col))
		b.WriteString("=$")
		b.WriteString(strconv.Itoa(columnIndex))
		return b.String()
	}
	for fieldName, val := range recordValues[0] {
		//primary key column
		if m.Key.Name == fieldName {
			currentColumnIndex++
			updateFields = append(updateFields, fieldName)
			updateInfo.Filters = append(updateInfo.Filters, newBind(fieldName, currentColumnIndex))
			valueExtractors = append(valueExtractors, identityVal)
			//cas column
		} else if fieldName == "cas" {

			currentColumnIndex++
			updateFields = append(updateFields, fieldName)
			updateInfo.Filters = append(updateInfo.Filters, newBind(fieldName, currentColumnIndex))
			valueExtractors = append(valueExtractors, identityVal)

			currentColumnIndex++
			updateFields = append(updateFields, fieldName)
			updateInfo.Values = append(updateInfo.Values, newBind(fieldName, currentColumnIndex))
			valueExtractors = append(valueExtractors, increaseCasVal)

		} else {
			switch val.(type) {
			case LazyLink:
				currentColumnIndex++
				updateFields = append(updateFields, fieldName)
				updateInfo.Filters = append(updateInfo.Filters, newBind(fieldName, currentColumnIndex))
				valueExtractors = append(valueExtractors, alinkVal)
			case DLink:
				currentColumnIndex++
				updateFields = append(updateFields, fieldName)
				updateInfo.Values = append(updateInfo.Values, newBind(fieldName, currentColumnIndex))
				valueExtractors = append(valueExtractors, dlinkVal)
			case *GenericInnerLink:

				currentColumnIndex++
				updateInfo.Values = append(updateInfo.Values, newBind(GetGenericFieldTypeColumnName(fieldName), currentColumnIndex))

				currentColumnIndex++
				updateInfo.Values = append(updateInfo.Values, newBind(GetGenericFieldKeyColumnName(fieldName), currentColumnIndex))

				updateFields = append(updateFields, fieldName)

				valueExtractors = append(valueExtractors, genericInnerLinkValue)
			default:
				currentColumnIndex++
				updateFields = append(updateFields, fieldName)
				updateInfo.Values = append(updateInfo.Values, newBind(fieldName, currentColumnIndex))
				valueExtractors = append(valueExtractors, identityVal)
			}
		}
	}

	if len(updateInfo.Values) == 0 {
		return emptyOperation, nil
	}

	if err := parsedTemplUpdate.Execute(&b, updateInfo); err != nil {
		logger.Error("Prepare update SQL by template error: %s", err.Error())
		return nil, errors.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}

	return func(dbTransaction transactions.DbTransaction) error {
		stmt, err := dbTransaction.(*PgTransaction).Prepare(b.String())
		if err != nil {
			return err
		}
		defer stmt.Close()

		for i := range recordValues {
			binds := make([]interface{}, 0)
			for j := range updateFields {
				if v, ok := recordValues[i][updateFields[j]]; !ok {
					return errors.NewValidationError(ErrInvalidArgument, "Different set of fields. Object #%d. All objects must have the same set of fields.", i)
				} else {
					value := valueExtractors[j](v)
					if value == nil {
						binds = append(binds, value)
					} else {
						switch castValue := value.(type) {
						case []string:
							for _, value := range castValue {
								binds = append(binds, value)
							}
						case []interface{}:
							//case for inner generic nil value: [nil,nil]
							for _, value := range castValue {
								switch value.(type) {
								case nil:
									binds = append(binds, value)
								default:
									binds = append(binds, fmt.Sprintf("%v", value))
								}

							}
						case interface{}:
							switch castValue.(type) {
							case nil:
								binds = append(binds, castValue)
							default:
								binds = append(binds, fmt.Sprintf("%v", castValue))
							}
						}
					}
				}
			}
			if uo, err := stmt.ParsedSingleQuery(binds, rFields); err == nil {
				updateNodes(recordValues[i], uo)
			} else {
				return err
			}
		}

		return nil
	}, nil
}

func (dm *DBManager) PrepareCreateOperation(m *Meta, recordsValues []map[string]interface{}) (transactions.Operation, error) {
	if len(recordsValues) == 0 {
		return emptyOperation, nil
	}

	//fix the columns by the first object
	fields := m.TableFields()

	insertFields, insertValuesPattern := utils.GetMapKeysValues(recordsValues[0])
	insertColumns := getColumnsToInsert(insertFields, insertValuesPattern)

	insertInfo := dml_info.NewInsertInfo(GetTableName(m.Name), insertColumns, getFieldsColumnsNames(fields), len(recordsValues))
	var insertDML bytes.Buffer
	if err := parsedTemplInsert.Execute(&insertDML, insertInfo); err != nil {
		return nil, errors.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}

	var fixSeqDML bytes.Buffer
	for _, field := range insertFields {
		if f := m.FindField(field); f != nil {
			def := f.Default()
			if d, ok := def.(description.DefExpr); ok && d.Func == "nextval" && f.Type == description.FieldTypeNumber {
				if err := parsedTemplFixSequense.Execute(&fixSeqDML, map[string]interface{}{
					"Table": insertInfo.Table,
					"Field": field,
				}); err != nil {
					return nil, errors.NewFatalError(ErrTemplateFailed, err.Error(), nil)
				}
			}
		}
	}

	return func(dbTransaction transactions.DbTransaction) error {
		//prepare binds only on executing step otherwise the foregin key may be absent (tx sequence)
		binds := make([]interface{}, 0, len(insertColumns)*len(recordsValues))
		for _, recordValues := range recordsValues {
			if values, err := getValuesToInsert(insertFields, recordValues, insertColumns); err != nil {
				return err
			} else {
				binds = append(binds, values...)
			}
		}
		stmt, err := NewStmt(dbTransaction.Transaction().(*sql.Tx), insertDML.String())
		if err != nil {
			return err
		}
		defer stmt.Close()
		dbObjs, err := stmt.ParsedQuery(binds, fields)
		if err != nil {
			if err, ok := err.(*errors.ServerError); ok && err.Code == ErrValueDuplication {
				//dupTransaction, _ := dbTransaction.(*PgTransaction).Manager.BeginTransaction()
				duplicates, dup_error := dm.GetAll(m, m.TableFields(), err.Data.(map[string]interface{}), dbTransaction)
				//dupTransaction.Close()
				if dup_error != nil {
					logger.Error(dup_error.Error())
				}
				err.Data = duplicates
			}
			return err
		}
		if _, err := dbTransaction.(*PgTransaction).Exec(fixSeqDML.String()); err != nil {
			return err
		}

		for i := 0; i < len(recordsValues); i++ {
			updateNodes(recordsValues[i], dbObjs[i])
		}
		return nil
	}, nil
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

func (dm *DBManager) Get(m *Meta, fields []*FieldDescription, key string, val interface{}, dbTransaction transactions.DbTransaction) (map[string]interface{}, error) {
	objs, err := dm.GetAll(m, fields, map[string]interface{}{key: val}, dbTransaction)
	if err != nil {
		return nil, err
	}

	l := len(objs)
	if l > 1 {
		return nil, errors.NewFatalError(ErrTooManyFound, "too many rows found", nil)
	}

	if l == 0 {
		return nil, nil
	}

	return objs[0], nil
}

func (dm *DBManager) GetAll(m *Meta, fields []*FieldDescription, filters map[string]interface{}, dbTransction transactions.DbTransaction) ([]map[string]interface{}, error) {
	tx := dbTransction.Transaction().(*sql.Tx)
	if fields == nil {
		fields = m.TableFields()
	}
	filterKeys, filterValues := GetMapKeysStrValues(filters)

	selectInfo := NewSelectInfo(m, fields, filterKeys)
	var q bytes.Buffer
	if err := selectInfo.sql(&q); err != nil {
		return nil, errors.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}

	stmt, err := dm.Prepare(q.String(), tx)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.ParsedQuery(filterValues, fields)
}

func (dm *DBManager) PerformRemove(recordNode *RecordRemovalNode, dbTransaction transactions.DbTransaction, notificationPool *RecordSetNotificationPool, processor *Processor) error {
	var operation transactions.Operation
	var err error
	var onDeleteStrategy description.OnDeleteStrategy
	if recordNode.OnDeleteStrategy != nil {
		onDeleteStrategy = *recordNode.OnDeleteStrategy
	} else {
		onDeleteStrategy = description.OnDeleteCascade
	}

	//make operation
	var recordSetNotification *RecordSetNotification
	switch onDeleteStrategy {
	case description.OnDeleteSetNull:
		//make corresponding null value
		var nullValue interface{}
		if recordNode.LinkField.Type == description.FieldTypeGeneric {
			nullValue = new(GenericInnerLink)
		} else {
			nullValue = nil
		}
		//update record with this value
		operation, err = dm.PrepareUpdateOperation(
			recordNode.Record.Meta,
			[]map[string]interface{}{{recordNode.Record.Meta.Key.Name: recordNode.Record.Pk(), recordNode.LinkField.Name: nullValue}},
		)
		if err != nil {
			return err
		}
		recordSetNotification = NewRecordSetNotification(&RecordSet{Meta: recordNode.Record.Meta, Records: []*Record{recordNode.Record}}, false, description.MethodUpdate, processor.GetBulk, processor.Get)
	default:
		operation, err = dm.PrepareRemoveOperation(recordNode.Record)
		if err != nil {
			return err
		}
		recordSetNotification = NewRecordSetNotification(&RecordSet{Meta: recordNode.Record.Meta, Records: []*Record{recordNode.Record}}, false, description.MethodRemove, processor.GetBulk, processor.Get)
	}
	//process child records
	for _, recordNodes := range recordNode.Children {
		for _, recordNode := range recordNodes {
			err := dm.PerformRemove(recordNode, dbTransaction, notificationPool, processor)
			if err != nil {
				return err
			}
		}
	}
	//create and process notification

	if recordSetNotification.ShouldBeProcessed() {
		recordSetNotification.CapturePreviousState()
		notificationPool.Add(recordSetNotification)
	}
	if err := dbTransaction.Execute([]transactions.Operation{operation}); err != nil {
		return err
	} else {
		recordSetNotification.CaptureCurrentState()
		return nil
	}
}

func (dm *DBManager) PrepareRemoveOperation(record *Record) (transactions.Operation, error) {
	var query bytes.Buffer
	deleteInfo := dml_info.NewDeleteInfo(GetTableName(record.Meta.Name), []string{dml_info.EscapeColumn(record.Meta.Key.Name) + " IN (" + dml_info.BindValues(1, 1) + ")"})
	operation := func(dbTransaction transactions.DbTransaction) error {
		stmt, err := dbTransaction.(*PgTransaction).Prepare(query.String())
		if err != nil {
			return err
		}
		defer stmt.Close()
		if _, err = stmt.Exec(record.Pk()); err != nil {
			return errors.NewFatalError(ErrDMLFailed, err.Error(), nil)
		}
		return nil
	}
	if err := parsedTemplDelete.Execute(&query, deleteInfo); err != nil {
		return nil, errors.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}
	return operation, nil

}

func (dm *DBManager) GetRql(dataNode *Node, rqlRoot *rqlParser.RqlRootNode, fields []*FieldDescription, dbTransaction transactions.DbTransaction) ([]map[string]interface{}, int, error) {
	tx := dbTransaction.Transaction().(*sql.Tx)
	tableAlias := string(dataNode.Meta.Name[0])
	translator := NewSqlTranslator(rqlRoot)
	sqlQuery, err := translator.query(tableAlias, dataNode)
	if err != nil {
		return nil, 0, err
	}

	selectInfo := &SelectInfo{
		From:   GetTableName(dataNode.Meta.Name) + " " + tableAlias,
		Cols:   fieldsToCols(fields, tableAlias),
		Where:  sqlQuery.Where,
		Order:  sqlQuery.Sort,
		Limit:  sqlQuery.Limit,
		Offset: sqlQuery.Offset,
	}

	countInfo := &SelectInfo{
		From:  GetTableName(dataNode.Meta.Name) + " " + tableAlias,
		Cols:  []string{"count(*)"},
		Where: sqlQuery.Where,
	}

	//records data
	var queryString bytes.Buffer
	if err := selectInfo.sql(&queryString); err != nil {
		return nil, 0, errors.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}
	statement, err := dm.Prepare(queryString.String(), tx)
	if err != nil {
		return nil, 0, err
	}
	defer statement.Close()
	//count data
	count := 0
	queryString.Reset()
	if err := countInfo.sql(&queryString); err != nil {
		return nil, 0, errors.NewFatalError(ErrTemplateFailed, err.Error(), nil)
	}
	countStatement, err := dm.Prepare(queryString.String(), tx)
	if err != nil {
		return nil, 0, err
	}
	defer countStatement.Close()

	recordsData, err := statement.ParsedQuery(sqlQuery.Binds, fields)
	err = countStatement.Scalar(&count, sqlQuery.Binds)
	if err != nil {
		return nil, 0, err
	}
	return recordsData, count, err
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
