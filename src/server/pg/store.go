package pg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"logger"
	"server/data"
	"server/meta"
	_ "github.com/lib/pq"
	"github.com/Q-CIS-DEV/go-rql-parser"
	"strconv"
	"strings"
	"text/template"
	"reflect"
	"server/pg/dml_info"
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

func fieldsNames(fields []*meta.FieldDescription) []string {
	fLen := len(fields)
	names := make([]string, fLen, fLen)
	for i := 0; i < fLen; i++ {
		names[i] = fields[i].Name
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

type Rows struct {
	*sql.Rows
}

func (rows *Rows) Parse(fields []*meta.FieldDescription) ([]map[string]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, NewDMLError(ErrDMLFailed, err.Error())
	}

	result := make([]map[string]interface{}, 0)
	i := 0
	fieldByName := func(name string) *meta.FieldDescription {
		for _, field := range fields {
			if field.Name == name {
				return field
			}
		}
		return nil
	}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			values[i], err = newFieldValue(fields[i], fields[i].Optional)
			if err != nil {
				return nil, err
			}
		}

		if err = rows.Scan(values...); err != nil {
			return nil, NewDMLError(ErrDMLFailed, err.Error())
		}
		result = append(result, make(map[string]interface{}))
		for j, n := range cols {
			if fieldByName(n).Type == meta.FieldTypeDate {
				switch value := values[j].(type) {
				case *sql.NullString:
					if value.Valid {
						result[i][n] = string([]rune(value.String)[0:10])
					} else {
						result[i][n] = nil
					}
				case *string:
					result[i][n] = string([]rune(*value)[0:10])
				}
			} else if fieldByName(n).Type == meta.FieldTypeTime {
				switch value := values[j].(type) {
				case *sql.NullString:
					if value.Valid {
						result[i][n] = string([]rune(value.String)[11:])
					} else {
						result[i][n] = nil
					}
				case *string:
					result[i][n] = string([]rune(*value)[11:])
				}
			} else if fieldByName(n).Type == meta.FieldTypeDateTime {
				switch value := values[j].(type) {
				case *sql.NullString:
					if value.Valid {
						result[i][n] = value.String
					} else {
						result[i][n] = nil
					}
				case *string:
					result[i][n] = *value
				}
			} else {
				switch t := values[j].(type) {
				case *string:
					result[i][n] = *t
				case *sql.NullString:
					if t.Valid {
						result[i][n] = t.String
					} else {
						result[i][n] = nil
					}
				case *float64:
					result[i][n] = *t
				case *sql.NullFloat64:
					if t.Valid {
						result[i][n] = t.Float64
					} else {
						result[i][n] = nil
					}
				case *bool:
					result[i][n] = *t
				case *sql.NullBool:
					if t.Valid {
						result[i][n] = t.Bool
					} else {
						result[i][n] = nil
					}
				default:
					return nil, NewDMLError(ErrDMLFailed, "unknown reference type '%s'", reflect.TypeOf(values[j]).String())
				}
			}
		}
		i++
	}
	return result, nil
}

func updateNodes(nodes map[string]interface{}, dbObj map[string]interface{}) {
	for col, rv := range dbObj {
		if val, ok := nodes[col]; ok {
			switch val := val.(type) {
			case data.ALink:
				continue
			case data.DLink:
				val.Id = rv
			default:
				nodes[col] = rv
			}
		} else {
			nodes[col] = rv
		}
	}
}

func keys(m map[string]interface{}) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
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

func identityVal(v interface{}) interface{} {
	return v
}

func increaseCasVal(v interface{}) interface{} {
	cas := v.(float64)
	return cas + 1
}

func (dataManager *DataManager) PrepareUpdates(m *meta.Meta, objs []map[string]interface{}) (data.Operation, error) {
	if len(objs) == 0 {
		return emptyOperation, nil
	}

	rFields := tableFields(m)
	updateInfo := dml_info.NewUpdateInfo(tblName(m), fieldsNames(rFields), make([]string, 0), make([]string, 0))
	cols := make([]string, 0, len(objs[0]))
	vals := make([]func(interface{}) interface{}, 0, len(objs[0]))
	var b bytes.Buffer
	newBind := func(col string) string {
		defer b.Reset()
		b.WriteString(fmt.Sprintf("\"%s\"", col))
		b.WriteString("=$")
		b.WriteString(strconv.Itoa(len(cols)))
		return b.String()
	}
	for col, val := range objs[0] {
		cols = append(cols, col)
		if m.Key.Name == col {
			updateInfo.Filters = append(updateInfo.Filters, newBind(col))
			vals = append(vals, identityVal)
		} else if col == "cas" {
			updateInfo.Filters = append(updateInfo.Filters, newBind(col))
			vals = append(vals, identityVal)

			cols = append(cols, col)
			updateInfo.Values = append(updateInfo.Values, newBind(col))
			vals = append(vals, increaseCasVal)
		} else {
			switch val.(type) {
			case data.ALink:
				updateInfo.Filters = append(updateInfo.Filters, newBind(col))
				vals = append(vals, alinkVal)
			case data.DLink:
				updateInfo.Values = append(updateInfo.Values, newBind(col))
				vals = append(vals, dlinkVal)
			default:
				updateInfo.Values = append(updateInfo.Values, newBind(col))
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

		binds := make([]interface{}, len(cols))
		for i := range objs {
			for j := range cols {
				if v, ok := objs[i][cols[j]]; !ok {
					return NewDMLError(ErrInvalidArgument, "Different set of fields. Object #%d. All objects must have the same set of fields.", i)
				} else {
					binds[j] = vals[j](v)
				}
			}
			if uo, err := stmt.ParsedSingleQuery(binds, rFields); err == nil {
				updateNodes(objs[i], uo)
			} else {
				if dml, ok := err.(*DMLError); ok && dml.code == ErrNotFound {
					return data.NewDataError("", data.ErrCasFailed, "Precondition failed on object #%d.", i)
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
	cols := keys(objs[0])
	insertInfo := dml_info.NewInsertInfo(tblName(m), cols, fieldsNames(fields), len(objs))
	var insertDML bytes.Buffer
	if err := parsedTemplInsert.Execute(&insertDML, insertInfo); err != nil {
		return nil, NewDMLError(ErrTemplateFailed, err.Error())
	}

	return func(ctx data.OperationContext) error {
		//prepare binds only on executing step otherwise the foregin key may be absent (db sequence)
		binds := make([]interface{}, 0, len(cols)*len(objs))
		for i, obj := range objs {
			if len(cols) != len(obj) {
				return NewDMLError(ErrInvalidArgument, "Different set of filds. Object #%d. All objects must have the same set of fileds.", i)
			}
			for _, col := range cols {
				if val, ok := obj[col]; ok {
					switch val := val.(type) {
					case data.ALink:
						binds = append(binds, val.Obj[val.Field.Meta.Key.Name])
					case data.DLink:
						binds = append(binds, val.Id)
					default:
						binds = append(binds, val)
					}
				} else {
					return NewDMLError(ErrInvalidArgument, "FieldDescription '%s' not found. Object #%d. All objects must have the same set of fileds.", col, i)
				}
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
	var cols = make([]string, len(fields), len(fields))
	if alias != "" {
		alias = alias + "."
	}
	for i, f := range fields {
		cols[i] = alias + f.Name
	}
	return cols
}

func (dataManager *DataManager) Get(m *meta.Meta, fields []*meta.FieldDescription, key string, val interface{}) (map[string]interface{}, error) {
	objs, err := dataManager.GetAll(m, fields, key, val)
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

func (dataManager *DataManager) GetAll(m *meta.Meta, fields []*meta.FieldDescription, key string, val interface{}) ([]map[string]interface{}, error) {
	if fields == nil {
		fields = tableFields(m)
	}

	selectInfo := &SelectInfo{From: tblName(m), Cols: fieldsToCols(fields, ""), Where: key + "=$1"}
	var q bytes.Buffer
	if err := selectInfo.sql(&q); err != nil {
		return nil, NewDMLError(ErrTemplateFailed, err.Error())
	}

	stmt, err := dataManager.Prepare(q.String())
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.ParsedQuery([]interface{}{val}, fields)
}

func (dataManager *DataManager) PrepareDelete(n *data.DNode, key interface{}) (data.Operation, []interface{}, error) {
	return dataManager.PrepareDeletes(n, []interface{}{key})
}

func (dataManager *DataManager) PrepareDeletes(n *data.DNode, keys []interface{}) (data.Operation, []interface{}, error) {
	var pks []interface{}
	if n.KeyFiled.Name != n.Meta.Key.Name {
		objs, err := dataManager.GetIn(n.Meta, []*meta.FieldDescription{n.Meta.Key}, n.KeyFiled.Name, keys)
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
	deleteInfo := dml_info.NewDeleteInfo(tblName(n.Meta), []string{sqlHelper.EscapeColumn(n.KeyFiled.Name) + " IN (" + sqlHelper.BindValues(1, len(keys)) + ")"})
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
	si := &SelectInfo{From: tblName(dataNode.Meta) + " " + tableAlias,
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
	si := &SelectInfo{From: tblName(m), Cols: fieldsToCols(fields, ""), Where: where.String()}
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
