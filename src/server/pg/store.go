package pg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"logger"
	"server/data"
	"server/data/errors"
	"server/object/meta"
	_ "github.com/lib/pq"
	"github.com/Q-CIS-DEV/go-rql-parser"
	"strconv"
	"strings"
	"text/template"
	"server/pg/dml_info"
	"server/data/types"
	"utils"
	"server/object/description"
	"server/transactions"
	"server/data/notifications"
	"server/data/record"
)

type DataManager struct {
	db *sql.DB
}

func (dm *DataManager) Db() interface{} {
	return dm.db
}

func NewDataManager(db *sql.DB) (*DataManager, error) {
	return &DataManager{db: db}, nil
}

type DMLError struct {
	code string
	msg  string
}

func (e *DMLError) Error() string {
	return fmt.Sprintf("DML error:  code='%s'  msg = '%s'", e.code, e.msg)
}

func (e *DMLError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"code": "dml:" + e.code,
		"msg":  e.msg,
	})
	return j
}

func NewDMLError(code string, msg string, a ...interface{}) *DMLError {
	return &DMLError{code: code, msg: fmt.Sprintf(msg, a...)}
}

const (
	ErrTrxFailed          = "transaction_failed"
	ErrTemplateFailed     = "template_failed"
	ErrInvalidArgument    = "invalid_argument"
	ErrDMLFailed          = "dml_failed"
	ErrConvertationFailed = "convertation_failed"
	ErrCommitFailed       = "commit_failed"
	ErrPreconditionFailed = "precondition_failed"
)

//{{ if isLast $key .Cols}}{{else}},{{end}}
const (
	templInsert = `INSERT INTO {{.Table}} {{if not .Cols}} DEFAULT VALUES {{end}}  {{if .Cols}} ({{join .Cols ", "}}) VALUES {{.GetValues}} {{end}} {{if .RCols}} RETURNING {{join .RCols ", "}}{{end}}`
	templSelect = `SELECT {{join .Cols ", "}} FROM {{.From}}{{if .Where}} WHERE {{.Where}}{{end}}{{if .Order}} ORDER BY {{.Order}}{{end}}{{if .Limit}} LIMIT {{.Limit}}{{end}}{{if .Offset}} OFFSET {{.Offset}}{{end}}`
	templDelete = `DELETE FROM {{.Table}}{{if .Filters}} WHERE {{join .Filters " AND "}}{{end}}`
	templUpdate = `UPDATE {{.Table}} SET {{join .Values ","}}{{if .Filters}} WHERE {{join .Filters " AND "}}{{end}}{{if .Cols}} RETURNING {{join .Cols ", "}}{{end}}`
)

var funcs = template.FuncMap{"join": strings.Join}
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

func NewSelectInfo(objectMeta *meta.Meta, fields []*meta.FieldDescription, filterKeys []string) *SelectInfo {
	sqlHelper := dml_info.SqlHelper{}
	whereExpression := ""
	for i, key := range filterKeys {
		if i > 0 {
			whereExpression += " AND "
		}
		whereExpression += sqlHelper.EscapeColumn(key) + "=$" + strconv.Itoa(i+1)
	}
	return &SelectInfo{From: GetTableName(objectMeta), Cols: sqlHelper.EscapeColumns(fieldsToCols(fields, "")), Where: whereExpression}
}

func tableFields(m *meta.Meta) []*meta.FieldDescription {
	fields := make([]*meta.FieldDescription, 0)
	l := len(m.Fields)
	for i := 0; i < l; i++ {
		if m.Fields[i].LinkType != description.LinkTypeOuter {
			fields = append(fields, &m.Fields[i])
		}
	}
	return fields
}

func getFieldsColumnsNames(fields []*meta.FieldDescription) []string {
	names := make([]string, 0)
	for _, field := range fields {
		switch field.Type {
		case description.FieldTypeGeneric:
			names = append(names, meta.GetGenericFieldTypeColumnName(field.Name))
			names = append(names, meta.GetGenericFieldKeyColumnName(field.Name))
		default:
			names = append(names, field.Name)
		}
	}
	return names
}

func newFieldValue(f *meta.FieldDescription, isOptional bool) (interface{}, error) {
	switch f.Type {
	case description.FieldTypeString, description.FieldTypeDate, description.FieldTypeDateTime, description.FieldTypeTime:
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
		return nil, NewDMLError(ErrConvertationFailed, "Unknown field type '%s'", f.Type)
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
		return nil, NewDMLError(ErrDMLFailed, err.Error())
	}
	logger.Debug("Prepared sql: %s", q)
	return &Stmt{statement}, nil
}

func (dataManager *DataManager) Prepare(q string, tx *sql.Tx) (*Stmt, error) {
	return NewStmt(tx, q)
}

func (s *Stmt) ParsedQuery(binds []interface{}, fields []*meta.FieldDescription) ([]map[string]interface{}, error) {
	rows, err := s.Query(binds)
	if err != nil {
		logger.Error("Query statement error: %s", err.Error())
		return nil, NewDMLError(ErrDMLFailed, err.Error())
	}
	defer rows.Close()
	return rows.Parse(fields)
}

func (s *Stmt) ParsedSingleQuery(binds []interface{}, fields []*meta.FieldDescription) (map[string]interface{}, error) {
	objs, err := s.ParsedQuery(binds, fields)
	if err != nil {
		return nil, err
	}

	count := len(objs)
	if count == 0 {
		return nil, NewDMLError(ErrNotFound, "No rows returned. Check the object's references, identificator and cas value.")
	} else if count > 1 {
		return nil, NewDMLError(ErrTooManyFound, "Mone then one rows returned")
	}

	return objs[0], nil

}

func (s *Stmt) Query(binds []interface{}) (*Rows, error) {
	rows, err := s.Stmt.Query(binds...)
	if err != nil {
		logger.Error("Execution statement error: %s\nBinds: %s", err.Error(), binds)
		return nil, NewDMLError(ErrDMLFailed, err.Error())
	}

	return &Rows{rows}, nil
}

func (s *Stmt) Scalar(receiver interface{}, binds []interface{}) (error) {
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
			case types.ALink:
				continue
			case types.DLink:
				val.Id = rv
			case *types.GenericInnerLink:
				if val.Pk != nil {
					nodes[fieldName] = map[string]interface{}{val.PkName: val.Pk, types.GenericInnerLinkObjectKey: val.ObjectName}
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
		case *types.GenericInnerLink:
			columns = append(columns, meta.GetGenericFieldTypeColumnName(fieldName))
			columns = append(columns, meta.GetGenericFieldKeyColumnName(fieldName))
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
		case *types.GenericInnerLink:
			values = append(values, castValue.ObjectName)
			values = append(values, castValue.Pk)
			processedColumns = append(processedColumns, meta.GetGenericFieldTypeColumnName(fieldName))
			processedColumns = append(processedColumns, meta.GetGenericFieldKeyColumnName(fieldName))
		case types.ALink:
			values = append(values, castValue.Obj[castValue.Field.Meta.Key.Name])
			processedColumns = append(processedColumns, fieldName)
		case types.DLink:
			values = append(values, castValue.Id)
			processedColumns = append(processedColumns, fieldName)
		default:
			values = append(values, castValue)
			processedColumns = append(processedColumns, fieldName)
		}
	}
	if !utils.Equal(expectedSetOfColumns, processedColumns, false) {
		return nil, NewDMLError(ErrInvalidArgument, "FieldDescription '%s' not found. All objects must have the same set of fileds.")
	}
	return values, nil
}

func emptyOperation(dbTransaction transactions.DbTransaction) error {
	return nil
}

func alinkVal(v interface{}) interface{} {
	al := v.(types.ALink)
	return al.Obj[al.Field.Meta.Key.Name]
}

func dlinkVal(v interface{}) interface{} {
	return v.(types.DLink).Id
}

func genericInnerLinkValue(value interface{}) interface{} {
	castValue := value.(*types.GenericInnerLink)
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

func (dataManager *DataManager) PrepareUpdateOperation(m *meta.Meta, recordValues []map[string]interface{}) (transactions.Operation, error) {
	if len(recordValues) == 0 {
		return emptyOperation, nil
	}

	rFields := tableFields(m)
	updateInfo := dml_info.NewUpdateInfo(GetTableName(m), getFieldsColumnsNames(rFields), make([]string, 0), make([]string, 0))
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
			case types.ALink:
				currentColumnIndex++
				updateFields = append(updateFields, fieldName)
				updateInfo.Filters = append(updateInfo.Filters, newBind(fieldName, currentColumnIndex))
				valueExtractors = append(valueExtractors, alinkVal)
			case types.DLink:
				currentColumnIndex++
				updateFields = append(updateFields, fieldName)
				updateInfo.Values = append(updateInfo.Values, newBind(fieldName, currentColumnIndex))
				valueExtractors = append(valueExtractors, dlinkVal)
			case *types.GenericInnerLink:

				currentColumnIndex++
				updateInfo.Values = append(updateInfo.Values, newBind(meta.GetGenericFieldTypeColumnName(fieldName), currentColumnIndex))

				currentColumnIndex++
				updateInfo.Values = append(updateInfo.Values, newBind(meta.GetGenericFieldKeyColumnName(fieldName), currentColumnIndex))

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

	if err := parsedTemplUpdate.Execute(&b, updateInfo); err != nil {
		logger.Error("Prepare update SQL by template error: %s", err.Error())
		return nil, NewDMLError(ErrTemplateFailed, err.Error())
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
					return NewDMLError(ErrInvalidArgument, "Different set of fields. Object #%d. All objects must have the same set of fields.", i)
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
								binds = append(binds, value)
							}
						case interface{}:
							binds = append(binds, castValue)
						}
					}
				}
			}
			if uo, err := stmt.ParsedSingleQuery(binds, rFields); err == nil {
				updateNodes(recordValues[i], uo)
			} else {
				if dml, ok := err.(*DMLError); ok && dml.code == ErrNotFound {
					return errors.NewDataError(m.Name, errors.ErrCasFailed, "Database has returned no rows. Probably CAS failed or record with such PK does not exist.", i)
				} else {
					return err
				}
			}
		}

		return nil
	}, nil
}

func (dataManager *DataManager) PrepareCreateOperation(m *meta.Meta, recordsValues []map[string]interface{}) (transactions.Operation, error) {
	if len(recordsValues) == 0 {
		return emptyOperation, nil
	}

	//fix the columns by the first object
	fields := tableFields(m)

	insertFields, insertValuesPattern := utils.GetMapKeysValues(recordsValues[0])
	insertColumns := getColumnsToInsert(insertFields, insertValuesPattern)

	insertInfo := dml_info.NewInsertInfo(GetTableName(m), insertColumns, getFieldsColumnsNames(fields), len(recordsValues))
	var insertDML bytes.Buffer
	if err := parsedTemplInsert.Execute(&insertDML, insertInfo); err != nil {
		return nil, NewDMLError(ErrTemplateFailed, err.Error())
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
		stmt, err := dbTransaction.(*PgTransaction).Prepare(insertDML.String())
		if err != nil {
			return err
		}
		defer stmt.Close()
		dbObjs, err := stmt.ParsedQuery(binds, fields)
		if err != nil {
			return err
		}

		for i := 0; i < len(recordsValues); i++ {
			updateNodes(recordsValues[i], dbObjs[i])
		}
		return nil
	}, nil
}

func fieldsToCols(fields []*meta.FieldDescription, alias string) []string {
	columns := getFieldsColumnsNames(fields)
	if alias != "" {
		alias = alias + "."
	}
	for i, column := range columns {
		columns[i] = alias + column
	}
	return columns
}

func (dataManager *DataManager) Get(m *meta.Meta, fields []*meta.FieldDescription, key string, val interface{}, dbTransaction transactions.DbTransaction) (map[string]interface{}, error) {
	objs, err := dataManager.GetAll(m, fields, map[string]interface{}{key: val}, dbTransaction)
	if err != nil {
		return nil, err
	}

	l := len(objs)
	if l > 1 {
		return nil, NewDMLError(ErrTooManyFound, "too many rows found")
	}

	if l == 0 {
		return nil, nil
	}

	return objs[0], nil
}

func (dataManager *DataManager) GetAll(m *meta.Meta, fields []*meta.FieldDescription, filters map[string]interface{}, dbTransction transactions.DbTransaction) ([]map[string]interface{}, error) {
	tx := dbTransction.Transaction().(*sql.Tx)
	if fields == nil {
		fields = tableFields(m)
	}
	filterKeys, filterValues := utils.GetMapKeysValues(filters)

	selectInfo := NewSelectInfo(m, fields, filterKeys)
	var q bytes.Buffer
	if err := selectInfo.sql(&q); err != nil {
		return nil, NewDMLError(ErrTemplateFailed, err.Error())
	}

	stmt, err := dataManager.Prepare(q.String(), tx)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.ParsedQuery(filterValues, fields)
}

func (dataManager *DataManager) PerformRemove(recordNode *data.RecordRemovalNode, dbTransaction transactions.DbTransaction, notificationPool *notifications.RecordSetNotificationPool, processor *data.Processor) (error) {
	var operation transactions.Operation
	var err error
	var onDeleteStrategy description.OnDeleteStrategy
	if recordNode.OnDeleteStrategy != nil {
		onDeleteStrategy = *recordNode.OnDeleteStrategy
	} else {
		onDeleteStrategy = description.OnDeleteCascade
	}

	//make operation
	var recordSetNotification *notifications.RecordSetNotification
	switch onDeleteStrategy {
	case description.OnDeleteSetNull:
		//make corresponding null value
		var nullValue interface{}
		if recordNode.LinkField.Type == description.FieldTypeGeneric {
			nullValue = new(types.GenericInnerLink)
		} else {
			nullValue = nil
		}
		//update record with this value
		operation, err = dataManager.PrepareUpdateOperation(
			recordNode.Record.Meta,
			[]map[string]interface{}{{recordNode.Record.Meta.Key.Name: recordNode.Record.Pk(), recordNode.LinkField.Name: nullValue}},
		)
		if err != nil {
			return err
		}
		recordSetNotification = notifications.NewRecordSetNotification(dbTransaction, &record.RecordSet{Meta: recordNode.Record.Meta, Records: []*record.Record{recordNode.Record}}, false, description.MethodUpdate, processor.GetBulk, processor.Get)
	default:
		var query bytes.Buffer
		sqlHelper := dml_info.SqlHelper{}
		deleteInfo := dml_info.NewDeleteInfo(GetTableName(recordNode.Record.Meta), []string{sqlHelper.EscapeColumn(recordNode.Record.Meta.Key.Name) + " IN (" + sqlHelper.BindValues(1, 1) + ")"})

		if err := parsedTemplDelete.Execute(&query, deleteInfo); err != nil {
			return NewDMLError(ErrTemplateFailed, err.Error())
		}

		operation = func(dbTransaction transactions.DbTransaction) error {
			stmt, err := dbTransaction.(*PgTransaction).Prepare(query.String())
			if err != nil {
				return err
			}
			defer stmt.Close()
			if _, err = stmt.Exec(recordNode.Record.Pk()); err != nil {
				return NewDMLError(ErrDMLFailed, err.Error())
			}
			return nil
		}
		recordSetNotification = notifications.NewRecordSetNotification(dbTransaction, &record.RecordSet{Meta: recordNode.Record.Meta, Records: []*record.Record{recordNode.Record}}, false, description.MethodRemove, processor.GetBulk, processor.Get)
	}
	//process child records
	for _, recordNodes := range recordNode.Children {
		for _, recordNode := range recordNodes {
			err := dataManager.PerformRemove(recordNode, dbTransaction, notificationPool, processor)
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

func (dataManager *DataManager) GetRql(dataNode *data.Node, rqlRoot *rqlParser.RqlRootNode, fields []*meta.FieldDescription, dbTransaction transactions.DbTransaction) ([]map[string]interface{}, int, error) {
	tx := dbTransaction.Transaction().(*sql.Tx)
	tableAlias := string(dataNode.Meta.Name[0])
	translator := NewSqlTranslator(rqlRoot)
	sqlQuery, err := translator.query(tableAlias, dataNode)
	if err != nil {
		return nil, 0, err
	}

	if fields == nil {
		fields = tableFields(dataNode.Meta)
	}
	selectInfo := &SelectInfo{
		From:   GetTableName(dataNode.Meta) + " " + tableAlias,
		Cols:   fieldsToCols(fields, tableAlias),
		Where:  sqlQuery.Where,
		Order:  sqlQuery.Sort,
		Limit:  sqlQuery.Limit,
		Offset: sqlQuery.Offset,
	}

	countInfo := &SelectInfo{
		From:  GetTableName(dataNode.Meta) + " " + tableAlias,
		Cols:  []string{"count(*)"},
		Where: sqlQuery.Where,
	}

	//records data
	var queryString bytes.Buffer
	if err := selectInfo.sql(&queryString); err != nil {
		return nil, 0, NewDMLError(ErrTemplateFailed, err.Error())
	}
	statement, err := dataManager.Prepare(queryString.String(), tx)
	if err != nil {
		return nil, 0, err
	}
	defer statement.Close()
	//count data
	count := 0
	queryString.Reset()
	if err := countInfo.sql(&queryString); err != nil {
		return nil, 0, NewDMLError(ErrTemplateFailed, err.Error())
	}
	countStatement, err := dataManager.Prepare(queryString.String(), tx)
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

func (dataManager *DataManager) GetIn(m *meta.Meta, fields []*meta.FieldDescription, key string, in []interface{}, dbTransaction transactions.DbTransaction) ([]map[string]interface{}, error) {
	if fields == nil {
		fields = tableFields(m)
	}
	sqlHelper := dml_info.SqlHelper{}
	where := bytes.NewBufferString(key)
	where.WriteString(" IN (")
	where.WriteString(sqlHelper.BindValues(1, len(in)))
	where.WriteString(")")
	si := &SelectInfo{From: GetTableName(m), Cols: fieldsToCols(fields, ""), Where: where.String()}
	var q bytes.Buffer
	if err := si.sql(&q); err != nil {
		return nil, NewDMLError(ErrTemplateFailed, err.Error())
	}

	stmt, err := dataManager.Prepare(q.String(), dbTransaction.Transaction().(*sql.Tx))
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.ParsedQuery(in, fields)
}
