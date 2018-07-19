package pg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"logger"
	"server/data"
	"server/data/errors"
	"server/meta"
	_ "github.com/lib/pq"
	"github.com/Q-CIS-DEV/go-rql-parser"
	"strconv"
	"strings"
	"text/template"
	"server/pg/dml_info"
	"server/data/types"
	"utils"
)

type DataManager struct {
	db *sql.DB
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

func NewSelectInfo(objectMeta *meta.Meta, fields []*meta.FieldDescription, filterKeys []interface{}) *SelectInfo {
	sqlHelper := dml_info.SqlHelper{}
	whereExpression := ""
	for i, key := range filterKeys {
		if i > 0 {
			whereExpression += " AND "
		}
		whereExpression += sqlHelper.EscapeColumn(key.(string)) + "=$" + strconv.Itoa(i+1)
	}
	return &SelectInfo{From: GetTableName(objectMeta), Cols: sqlHelper.EscapeColumns(fieldsToCols(fields, "")), Where: whereExpression}
}

func tableFields(m *meta.Meta) []*meta.FieldDescription {
	fields := make([]*meta.FieldDescription, 0)
	l := len(m.Fields)
	for i := 0; i < l; i++ {
		if m.Fields[i].LinkType != meta.LinkTypeOuter {
			fields = append(fields, &m.Fields[i])
		}
	}
	return fields
}

func getFieldsColumnsNames(fields []*meta.FieldDescription) []string {
	names := make([]string, 0)
	for _, field := range fields {
		switch field.Type {
		case meta.FieldTypeGeneric:
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
	case meta.FieldTypeString, meta.FieldTypeDate, meta.FieldTypeDateTime, meta.FieldTypeTime:
		if isOptional {
			return new(sql.NullString), nil
		} else {
			return new(string), nil
		}
	case meta.FieldTypeNumber:
		if isOptional {
			return new(sql.NullFloat64), nil
		} else {
			return new(float64), nil
		}
	case meta.FieldTypeBool:
		if isOptional {
			return new(sql.NullBool), nil
		} else {
			return new(bool), nil
		}
	case meta.FieldTypeObject, meta.FieldTypeArray:
		if f.LinkType == meta.LinkTypeInner {
			return newFieldValue(f.LinkMeta.Key, f.Optional)
		} else {
			return newFieldValue(f.OuterLinkField, f.Optional)
		}
	case meta.FieldTypeGeneric:
		if f.LinkType == meta.LinkTypeInner {
			return []interface{}{new(sql.NullString), new(sql.NullString)}, nil
		} else {
			return newFieldValue(f.OuterLinkField, f.Optional)
		}
	default:
		return nil, NewDMLError(ErrConvertationFailed, "Unknown field type '%s'", f.Type)
	}
}

type pgOpCtx struct {
	tx *Tx
}

type Stmt struct {
	*sql.Stmt
}

type StmtPreparer interface {
	Prepare(string) (*sql.Stmt, error)
}

func NewStmt(sp StmtPreparer, q string) (*Stmt, error) {
	statement, err := sp.Prepare(q)
	if err != nil {
		logger.Error("Prepare statement error: %s %s", q, err.Error())
		return nil, NewDMLError(ErrDMLFailed, err.Error())
	}
	logger.Debug("Prepared sql: %s", q)
	return &Stmt{statement}, nil
}

func (dataManager *DataManager) Prepare(q string) (*Stmt, error) {
	return NewStmt(dataManager.db, q)
}

type Tx struct {
	*sql.Tx
}

func (tx *Tx) Prepare(q string) (*Stmt, error) {
	return NewStmt(tx.Tx, q)
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

func updateNodes(nodes map[string]interface{}, dbObj map[string]interface{}) {
	for fieldName, rv := range dbObj {
		if val, ok := nodes[fieldName]; ok {
			switch val := val.(type) {
			case data.ALink:
				continue
			case data.DLink:
				val.Id = rv
			case *types.GenericInnerLink:
				nodes[fieldName] = map[string]string{val.PkName: val.Pk, types.GenericInnerLinkObjectKey: val.ObjectName}
			default:
				nodes[fieldName] = rv
			}
		} else {
			nodes[fieldName] = rv
		}
	}
}

func getColumnsToInsert(m map[string]interface{}) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		switch m[k].(type) {
		case *types.GenericInnerLink:
			ks = append(ks, meta.GetGenericFieldTypeColumnName(k))
			ks = append(ks, meta.GetGenericFieldKeyColumnName(k))
		default:
			ks = append(ks, k)
		}
	}
	return ks
}

func getValuesToInsert(rawValues map[string]interface{}, columns []string) ([]interface{}, error) {
	values := make([]interface{}, 0)
	processedColumns := make([]string, 0)
	for key, value := range rawValues {
		switch castValue := value.(type) {
		case *types.GenericInnerLink:
			values = append(values, castValue.ObjectName)
			values = append(values, castValue.Pk)
			processedColumns = append(processedColumns, meta.GetGenericFieldTypeColumnName(key))
			processedColumns = append(processedColumns, meta.GetGenericFieldKeyColumnName(key))
		case data.ALink:
			values = append(values, castValue.Obj[castValue.Field.Meta.Key.Name])
			processedColumns = append(processedColumns, key)
		case data.DLink:
			values = append(values, castValue.Id)
			processedColumns = append(processedColumns, key)
		default:
			values = append(values, castValue)
			processedColumns = append(processedColumns, key)
		}
	}
	if !utils.Equal(columns, processedColumns, false) {
		return nil, NewDMLError(ErrInvalidArgument, "FieldDescription '%s' not found. All objects must have the same set of fileds.")
	}
	return values, nil
}

func emptyOperation(ctx data.OperationContext) error {
	return nil
}

func alinkVal(v interface{}) interface{} {
	al := v.(data.ALink)
	return al.Obj[al.Field.Meta.Key.Name]
}

func dlinkVal(v interface{}) interface{} {
	return v.(data.DLink).Id
}

func genericInnerLinkValue(value interface{}) interface{} {
	castValue := value.(*types.GenericInnerLink)
	return []string{castValue.ObjectName, castValue.Pk}
}

func identityVal(v interface{}) interface{} {
	return v
}

func increaseCasVal(v interface{}) interface{} {
	cas := v.(float64)
	return cas + 1
}

func (dataManager *DataManager) PrepareUpdates(m *meta.Meta, recordValues []map[string]interface{}) (data.Operation, error) {
	if len(recordValues) == 0 {
		return emptyOperation, nil
	}

	rFields := tableFields(m)
	updateInfo := dml_info.NewUpdateInfo(GetTableName(m), getFieldsColumnsNames(rFields), make([]string, 0), make([]string, 0))
	updateFields := make([]string, 0, len(recordValues[0]))
	vals := make([]func(interface{}) interface{}, 0, len(recordValues[0]))
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
			vals = append(vals, identityVal)
			//cas column
		} else if fieldName == "cas" {

			currentColumnIndex++
			updateFields = append(updateFields, fieldName)
			updateInfo.Filters = append(updateInfo.Filters, newBind(fieldName, currentColumnIndex))
			vals = append(vals, identityVal)

			currentColumnIndex++
			updateFields = append(updateFields, fieldName)
			updateInfo.Values = append(updateInfo.Values, newBind(fieldName, currentColumnIndex))
			vals = append(vals, increaseCasVal)

		} else {
			switch val.(type) {
			case data.ALink:
				currentColumnIndex++
				updateFields = append(updateFields, fieldName)
				updateInfo.Filters = append(updateInfo.Filters, newBind(fieldName, currentColumnIndex))
				vals = append(vals, alinkVal)
			case data.DLink:
				currentColumnIndex++
				updateFields = append(updateFields, fieldName)
				updateInfo.Values = append(updateInfo.Values, newBind(fieldName, currentColumnIndex))
				vals = append(vals, dlinkVal)
			case *types.GenericInnerLink:

				currentColumnIndex++
				updateInfo.Values = append(updateInfo.Values, newBind(meta.GetGenericFieldTypeColumnName(fieldName), currentColumnIndex))

				currentColumnIndex++
				updateInfo.Values = append(updateInfo.Values, newBind(meta.GetGenericFieldKeyColumnName(fieldName), currentColumnIndex))

				updateFields = append(updateFields, fieldName)

				vals = append(vals, genericInnerLinkValue)
			default:
				currentColumnIndex++
				updateFields = append(updateFields, fieldName)
				updateInfo.Values = append(updateInfo.Values, newBind(fieldName, currentColumnIndex))
				vals = append(vals, identityVal)
			}
		}
	}

	if err := parsedTemplUpdate.Execute(&b, updateInfo); err != nil {
		logger.Error("Prepare update SQL by template error: %s", err.Error())
		return nil, NewDMLError(ErrTemplateFailed, err.Error())
	}

	return func(ctx data.OperationContext) error {
		stmt, err := ctx.(*pgOpCtx).tx.Prepare(b.String())
		if err != nil {
			return err
		}
		defer stmt.Close()

		binds := make([]interface{}, 0)
		for i := range recordValues {
			for j := range updateFields {
				if v, ok := recordValues[i][updateFields[j]]; !ok {
					return NewDMLError(ErrInvalidArgument, "Different set of fields. Object #%d. All objects must have the same set of fields.", i)
				} else {
					value := vals[j](v)
					switch castValue := value.(type) {
					case []string:
						for _, value := range castValue {
							binds = append(binds, value)
						}
					case interface{}:
						binds = append(binds, castValue)
					}
				}
			}
			if uo, err := stmt.ParsedSingleQuery(binds, rFields); err == nil {
				updateNodes(recordValues[i], uo)
			} else {
				if dml, ok := err.(*DMLError); ok && dml.code == ErrNotFound {
					return errors.NewDataError("", errors.ErrCasFailed, "Precondition failed on object #%d.", i)
				} else {
					return err
				}
			}
		}

		return nil
	}, nil
}

func (dataManager *DataManager) PreparePuts(m *meta.Meta, objs []map[string]interface{}) (data.Operation, error) {
	if len(objs) == 0 {
		return emptyOperation, nil
	}

	//fix the columns by the first object
	fields := tableFields(m)
	insertColumns := getColumnsToInsert(objs[0])
	insertInfo := dml_info.NewInsertInfo(GetTableName(m), insertColumns, getFieldsColumnsNames(fields), len(objs))
	var insertDML bytes.Buffer
	if err := parsedTemplInsert.Execute(&insertDML, insertInfo); err != nil {
		return nil, NewDMLError(ErrTemplateFailed, err.Error())
	}

	return func(ctx data.OperationContext) error {
		//prepare binds only on executing step otherwise the foregin key may be absent (tx sequence)
		binds := make([]interface{}, 0, len(insertColumns)*len(objs))
		for _, obj := range objs {
			if values, err := getValuesToInsert(obj, insertColumns); err != nil {
				return err
			} else {
				binds = append(binds, values...)
			}
		}
		stmt, err := ctx.(*pgOpCtx).tx.Prepare(insertDML.String())
		if err != nil {
			return err
		}
		defer stmt.Close()
		dbObjs, err := stmt.ParsedQuery(binds, fields)
		if err != nil {
			return err
		}

		for i := 0; i < len(objs); i++ {
			updateNodes(objs[i], dbObjs[i])
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

func (dataManager *DataManager) Get(m *meta.Meta, fields []*meta.FieldDescription, key string, val interface{}) (map[string]interface{}, error) {
	objs, err := dataManager.GetAll(m, fields, map[string]interface{}{key: val})
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

func (dataManager *DataManager) GetAll(m *meta.Meta, fields []*meta.FieldDescription, filters map[string]interface{}) ([]map[string]interface{}, error) {
	if fields == nil {
		fields = tableFields(m)
	}
	filterKeys, filterValues := utils.GetMapKeysValues(filters)

	selectInfo := NewSelectInfo(m, fields, filterKeys)
	var q bytes.Buffer
	if err := selectInfo.sql(&q); err != nil {
		return nil, NewDMLError(ErrTemplateFailed, err.Error())
	}

	stmt, err := dataManager.Prepare(q.String())
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.ParsedQuery(filterValues, fields)
}

func (dataManager *DataManager) PrepareDelete(n *data.DNode, key interface{}) (data.Operation, []interface{}, error) {
	return dataManager.PrepareDeletes(n, []interface{}{key})
}

func (dataManager *DataManager) PrepareDeletes(n *data.DNode, keys []interface{}) (data.Operation, []interface{}, error) {
	var pks []interface{}
	if n.KeyField.Name != n.Meta.Key.Name {
		objs, err := dataManager.GetIn(n.Meta, []*meta.FieldDescription{n.Meta.Key}, n.KeyField.Name, keys)
		if err != nil {
			return nil, nil, err
		}
		pks := make([]interface{}, len(objs), len(objs))
		for i := range objs {
			pks[i] = objs[i][n.Meta.Key.Name]
		}
	} else {
		pks = keys
	}
	sqlHelper := dml_info.SqlHelper{}
	deleteInfo := dml_info.NewDeleteInfo(GetTableName(n.Meta), []string{sqlHelper.EscapeColumn(n.KeyField.Name) + " IN (" + sqlHelper.BindValues(1, len(keys)) + ")"})
	var q bytes.Buffer
	if err := parsedTemplDelete.Execute(&q, deleteInfo); err != nil {
		return nil, nil, NewDMLError(ErrTemplateFailed, err.Error())
	}

	return func(ctx data.OperationContext) error {
		stmt, err := ctx.(*pgOpCtx).tx.Prepare(q.String())
		if err != nil {
			return err
		}
		defer stmt.Close()
		if _, err = stmt.Exec(keys...); err != nil {
			return NewDMLError(ErrDMLFailed, err.Error())
		}
		return nil
	}, pks, nil
}

type ExecuteContext struct {
	tx *Tx
}

func (ex *ExecuteContext) Execute(ops []data.Operation) error {
	ctx := &pgOpCtx{tx: ex.tx}
	for _, op := range ops {
		if err := op(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (ex *ExecuteContext) Complete() error {
	if err := ex.tx.Commit(); err != nil {
		return NewDMLError(ErrCommitFailed, err.Error())
	}
	return nil
}

func (ex *ExecuteContext) Close() error {
	return ex.tx.Rollback()
}

func (dataManager *DataManager) ExecuteContext() (data.ExecuteContext, error) {
	tx, err := dataManager.db.Begin()
	if err != nil {
		return nil, NewDMLError(ErrTrxFailed, err.Error())
	}

	return &ExecuteContext{tx: &Tx{tx}}, nil
}

func (dataManager *DataManager) Execute(ops []data.Operation) error {
	ex, err := dataManager.ExecuteContext()
	if err != nil {
		return err
	}
	defer ex.Close()

	if err := ex.Execute(ops); err != nil {
		return err
	}

	return ex.Complete()
}

func (dataManager *DataManager) GetRql(dataNode *data.Node, rqlRoot *rqlParser.RqlRootNode, fields []*meta.FieldDescription) ([]map[string]interface{}, error) {
	tableAlias := string(dataNode.Meta.Name[0])
	translator := NewSqlTranslator(rqlRoot)
	sqlQuery, err := translator.query(tableAlias, dataNode)
	if err != nil {
		return nil, err
	}

	if fields == nil {
		fields = tableFields(dataNode.Meta)
	}
	si := &SelectInfo{From: GetTableName(dataNode.Meta) + " " + tableAlias,
		Cols: fieldsToCols(fields, tableAlias),
		Where: sqlQuery.Where,
		Order: sqlQuery.Sort,
		Limit: sqlQuery.Limit,
		Offset: sqlQuery.Offset}

	var queryString bytes.Buffer
	if err := si.sql(&queryString); err != nil {
		return nil, NewDMLError(ErrTemplateFailed, err.Error())
	}

	statement, err := dataManager.Prepare(queryString.String())
	if err != nil {
		return nil, err
	}
	defer statement.Close()

	return statement.ParsedQuery(sqlQuery.Binds, fields)
}

func (dataManager *DataManager) GetIn(m *meta.Meta, fields []*meta.FieldDescription, key string, in []interface{}) ([]map[string]interface{}, error) {
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

	stmt, err := dataManager.Prepare(q.String())
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.ParsedQuery(in, fields)
}
