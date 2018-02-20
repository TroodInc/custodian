package pg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	//	"git.reaxoft.loc/infomir/custodian/logger"
	"github.com/Q-CIS-DEV/custodian/server/meta"
	"regexp"
	"strconv"
	"strings"
	"text/template"
)

// Meta DDL errors
const (
	ErrUnsupportedColumnType = "unsuported_column_type"
	ErrUnsupportedLinkType   = "unsuported_link_type"
	ErrNotFound              = "not_found"
	ErrTooManyFound          = "too_many_found"
	ErrInternal              = "internal"
	ErrWrongDefultValue      = "wrong_default_value"
	ErrExecutingDDL          = "error_exec_ddl"
)

type DDLError struct {
	code  string
	msg   string
	table string
}

func (e *DDLError) Error() string {
	return fmt.Sprintf("DDL error:  table = '%s', code='%s'  msg = '%s'", e.table, e.code, e.msg)
}

func (e *DDLError) Json() []byte {
	j, _ := json.Marshal(map[string]string{
		"table": e.table,
		"code":  "table:" + e.code,
		"msg":   e.msg,
	})
	return j
}

//DDL statament description
type DDLStmt struct {
	Name string
	Code string
	err  error
	time int
}

//Collection of the DDL statements
type DDLStmts []*DDLStmt

//Adds a DDL statement to the colletcion of them
func (ds *DDLStmts) Add(s *DDLStmt) {
	*ds = append(*ds, s)
}

// DDL column type
type ColumnType int

const (
	ColumnTypeText ColumnType = iota + 1
	ColumnTypeNumeric
	ColumnTypeBool
)

func (ct ColumnType) DdlType() (string, error) {
	switch ct {
	case ColumnTypeText:
		return "text", nil
	case ColumnTypeNumeric:
		return "numeric", nil
	case ColumnTypeBool:
		return "bool", nil
	default:
		return "", &DDLError{code: ErrUnsupportedColumnType, msg: "Unsupported column type: " + string(ct)}
	}
}

func fieldTypeToColumnType(ft meta.FieldType) (ColumnType, bool) {
	switch ft {
	case meta.FieldTypeString:
		return ColumnTypeText, true
	case meta.FieldTypeNumber:
		return ColumnTypeNumeric, true
	case meta.FieldTypeBool:
		return ColumnTypeBool, true
	default:
		return 0, false
	}
}

func dbTypeToColumnType(dt string) (ColumnType, bool) {
	switch dt {
	case "text":
		return ColumnTypeText, true
	case "numeric":
		return ColumnTypeNumeric, true
	case "boolean":
		return ColumnTypeBool, true
	default:
		return 0, false
	}
}

//DDL table metadata
type MetaDDL struct {
	Table   string
	Columns []Column
	Pk      string
	IFKs    []IFK
	OFKs    []OFK
	Seqs    []Seq
}

// DDL column meta
type Column struct {
	Name     string
	Typ      ColumnType
	Optional bool
	Unique   bool
	Defval   string
}

type IFK struct {
	FromColumn string
	ToTable    string
	ToColumn   string
}

type OFK struct {
	FromTable  string
	FromColumn string
	ToColumn   string
	ToTable    string
}

type Seq struct {
	Name string
}

type ColDefVal interface {
	ddlVal() (string, error)
}

type ColDefValEmpty struct{}

func (empty *ColDefValEmpty) ddlVal() (string, error) {
	return "", nil
}

var colDefValEmpty = ColDefValEmpty{}

type ColDefValSimple struct {
	val string
}

func newColDefValSimple(v interface{}) (*ColDefValSimple, error) {
	if s, err := valToDdl(v); err == nil {
		return &ColDefValSimple{s}, nil
	} else {
		return nil, err
	}
}
func (simple *ColDefValSimple) ddlVal() (string, error) {
	return simple.val, nil
}

type ColDefValSeq struct {
	seq *Seq
}

func (c *ColDefValSeq) ddlVal() (string, error) {
	return "nextval('" + c.seq.Name + "')", nil
}

type ColDefValFunc struct {
	*meta.DefExpr
}

func (dFunc *ColDefValFunc) ddlVal() (string, error) {
	args := make([]string, len(dFunc.Args), len(dFunc.Args))
	var err error
	for i, _ := range dFunc.Args {
		if args[i], err = valToDdl(dFunc.Args[i]); err != nil {
			return "", err
		}
	}
	return dFunc.Func + "(" + strings.Join(args, ",") + ")", nil
}

//Auxilary template functions
var ddlFuncs = template.FuncMap{"dict": dictionary}

func valToDdl(v interface{}) (string, error) {
	switch v := v.(type) {
	case string:
		return "\"" + v + "\"", nil
	case uint8:
		return string(v), nil
	case uint16:
		return string(v), nil
	case uint32:
		return string(v), nil
	case uint64:
		return string(v), nil
	case int8:
		return string(v), nil
	case int16:
		return string(v), nil
	case int32:
		return string(v), nil
	case int64:
		return string(v), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		if v {
			return "true", nil
		} else {
			return "false", nil
		}
	default:
		return "", &DDLError{code: ErrWrongDefultValue, msg: "Wrong value type"}
	}
}

func newFieldSeq(f *meta.Field, args []interface{}) (*Seq, error) {
	if len(args) > 0 {
		if name, err := valToDdl(args[0]); err == nil {
			return &Seq{Name: name}, nil
		} else {
			return nil, err
		}
	} else {
		return &Seq{Name: tblName(f.Meta) + "_" + f.Name + "_seq"}, nil
	}
}

func defaultNextval(f *meta.Field, args []interface{}) (ColDefVal, error) {
	if s, err := newFieldSeq(f, args); err == nil {
		return &ColDefValSeq{s}, nil
	} else {
		return nil, err
	}
}

var defaultFuncs = map[string]func(f *meta.Field, args []interface{}) (ColDefVal, error){
	"nextval": defaultNextval,
}

func newColDefVal(f *meta.Field) (ColDefVal, error) {
	if def := f.Default(); def != nil {
		switch v := def.(type) {
		case meta.DefConstStr:
			return newColDefValSimple(v.Value)
		case meta.DefConstNum:
			return newColDefValSimple(v.Value)
		case meta.DefConstBool:
			return newColDefValSimple(v.Value)
		case meta.DefExpr:
			if fn, ok := defaultFuncs[strings.ToLower(v.Func)]; ok {
				return fn(f, v.Args)
			} else {
				return &ColDefValFunc{&v}, nil
			}
		default:
			return nil, &DDLError{code: ErrWrongDefultValue, msg: "Wrong defult value"}
		}
	} else {
		return &colDefValEmpty, nil
	}
}

func dictionary(values ...interface{}) (map[string]interface{}, error) {
	if len(values)&1 != 0 {
		return nil, errors.New("count of arguments must be even")
	}
	dict := make(map[string]interface{}, len(values)>>1)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			return nil, errors.New("dictionary key must be of string type")
		}
		dict[key] = values[i+1]
	}
	return dict, nil
}

//DDL create table templates
const (
	templCreateTable = `CREATE TABLE {{.Table}} (
	{{range .Columns}}{{template "column" .}},{{"\n"}}{{end}}{{$mtable:=.Table}}{{range .IFKs}}{{template "ifk" dict "Mtable" $mtable "dot" .}},{{"\n"}}{{end}}PRIMARY KEY ({{.Pk}})
    );`
	templCreateTableColumns = `{{define "column"}}{{.Name}} {{.Typ.DdlType}}{{if not .Optional}} NOT NULL{{end}}{{if .Unique}} UNIQUE{{end}}{{if .Defval}} DEFAULT {{.Defval}}{{end}}{{end}}`
	templCreateTableInnerFK = `{{define "ifk"}}CONSTRAINT fk_{{.dot.FromColumn}}_{{.dot.ToTable}}_{{.dot.ToColumn}} FOREIGN KEY ({{.dot.FromColumn}}) REFERENCES {{.dot.ToTable}} ({{.dot.ToColumn}}){{end}}`
)

var parsedTemplCreateTable = template.Must(template.Must(template.Must(template.New("create_table_ddl").Funcs(ddlFuncs).Parse(templCreateTable)).Parse(templCreateTableColumns)).Parse(templCreateTableInnerFK))

//Creates a DDL script to make a table based on the metadata
func (md *MetaDDL) createTableScript() (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplCreateTable.Execute(&buffer, md); e != nil {
		return nil, &DDLError{table: md.Table, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: "create_table#" + md.Table, Code: buffer.String()}, nil
}

//DDL drop table template
const templDropTable95 = `DROP TABLE IF EXISTS {{.Table}};`
const templDropTable94 = `DROP TABLE {{.Table}};`

var parsedTemplDropTable = template.Must(template.New("drop_table").Funcs(ddlFuncs).Parse(templDropTable94))

//Creates a DDL to drop a table
func (md *MetaDDL) dropTableScript() (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplDropTable.Execute(&buffer, md); e != nil {
		return nil, &DDLError{table: md.Table, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: "drop_table#" + md.Table, Code: buffer.String()}, nil
}

//DDL drop table column template
const templDropTableColumn = `ALTER TABLE {{.Table}} DROP COLUMN {{.dot.Name}};`

var parsedTemplDropTableColumn = template.Must(template.New("drop_table_column").Funcs(ddlFuncs).Parse(templDropTableColumn))

//Creates a DDL to drop a table's column
func (cl *Column) dropScript(tname string) (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplDropTableColumn.Execute(&buffer, map[string]interface{}{
		"Table": tname,
		"dot":   cl}); e != nil {
		return nil, &DDLError{table: tname, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: fmt.Sprintf("drop_table_column#%s.%s", tname, cl.Name), Code: buffer.String()}, nil
}

//DDL add table column template
const templAddTableColumn = `ALTER TABLE {{.Table}} ADD COLUMN {{.dot.Name}} {{.dot.Typ.DdlType}}{{if not .dot.Optional}} NOT NULL{{end}}{{if .dot.Unique}} UNIQUE{{end}}{{if .dot.Defval}} DEFAULT {{.dot.Defval}}{{end}};`

var parsedTemplAddTableColumn = template.Must(template.New("add_table_column").Funcs(ddlFuncs).Parse(templAddTableColumn))

//Creates a DDL to add a table's column
func (cl *Column) addScript(tname string) (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplAddTableColumn.Execute(&buffer, map[string]interface{}{
		"Table": tname,
		"dot":   cl}); e != nil {
		return nil, &DDLError{table: tname, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: fmt.Sprintf("add_table_column#%s.%s", tname, cl.Name), Code: buffer.String()}, nil
}

//DDL create table outer foreign key templates
const templCreateOuterFK = `ALTER TABLE {{.FromTable}} ADD CONSTRAINT fk_{{.FromColumn}}_{{.ToTable}}_{{.ToColumn}} FOREIGN KEY ({{.FromColumn}}) REFERENCES {{.ToTable}} ({{.ToColumn}});`

var parsedTemplCreateFK = template.Must(template.New("create_ofk_ddl").Funcs(ddlFuncs).Parse(templCreateOuterFK))

//Creates  a DDL script to make outer foreign key
func (fk *OFK) createScript() (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplCreateFK.Execute(&buffer, fk); e != nil {
		return nil, &DDLError{table: fk.ToTable, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: fmt.Sprintf("create_ofk#%s_%s_%s_%s", fk.FromTable, fk.FromColumn, fk.ToTable, fk.ToColumn), Code: buffer.String()}, nil
}

//DDL drop table outer foreign key template
const templDropOuterFK = `ALTER TABLE {{.FromTable}} DROP CONSTRAINT fk_{{.FromColumn}}_{{.ToTable}}_{{.ToColumn}};`

var parsedTemplDropOuterFK = template.Must(template.New("drop_ofk").Funcs(ddlFuncs).Parse(templDropOuterFK))

//Creates a DDL script to remove outer foreign key
func (fk *OFK) dropScript() (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplDropOuterFK.Execute(&buffer, fk); e != nil {
		return nil, &DDLError{table: fk.ToTable, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: fmt.Sprintf("drop_ofk#%s_%s_%s_%s", fk.FromTable, fk.FromColumn, fk.ToTable, fk.ToColumn), Code: buffer.String()}, nil
}

//DDL drop table inner foreign key template
const templDropInnerFK = `ALTER TABLE {{.Table}} DROP CONSTRAINT fk_{{.dot.FromColumn}}_{{.dot.ToTable}}_{{.dot.ToColumn}};`

var parsedTemplDropInnerFK = template.Must(template.New("drop_ifk").Funcs(ddlFuncs).Parse(templDropInnerFK))

//Creates a DDL script to remove inner foreign key
func (fk *IFK) dropScript(tname string) (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplDropInnerFK.Execute(&buffer, map[string]interface{}{
		"Table": tname,
		"dot":   fk}); e != nil {
		return nil, &DDLError{table: tname, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: fmt.Sprintf("drop_ifk#%s_%s_%s_%s", tname, fk.FromColumn, fk.ToTable, fk.ToColumn), Code: buffer.String()}, nil
}

//DDL add table inner foreign key template
const templAddInnerFK = `ALTER TABLE {{.Table}} ADD CONSTRAINT fk_{{.dot.FromColumn}}_{{.dot.ToTable}}_{{.dot.ToColumn}} FOREIGN KEY ({{.dot.FromColumn}}) REFERENCES {{.dot.ToTable}} ({{.dot.ToColumn}});`

var parsedTemplAddInnerFK = template.Must(template.New("add_ifk").Funcs(ddlFuncs).Parse(templAddInnerFK))

//Creates a DDL script to add inner foreign key
func (fk *IFK) addScript(tname string) (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplAddInnerFK.Execute(&buffer, map[string]interface{}{
		"Table": tname,
		"dot":   fk}); e != nil {
		return nil, &DDLError{table: tname, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: fmt.Sprintf("add_ifk#%s_%s_%s_%s", tname, fk.FromColumn, fk.ToTable, fk.ToColumn), Code: buffer.String()}, nil
}

//DDL scripts to create sequence
const templCreateSeq94 = `CREATE SEQUENCE {{.Name}};`
const templCreateSeq95 = `CREATE SEQUENCE IF NOT EXISTS {{.Name}};`

var parsedTemplCreateSeq = template.Must(template.New("add_seq").Funcs(ddlFuncs).Parse(templCreateSeq94))

func (s *Seq) createScript() (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplCreateSeq.Execute(&buffer, s); e != nil {
		return nil, &DDLError{table: s.Name, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: fmt.Sprintf("create_seq#%s", s.Name), Code: buffer.String()}, nil
}

//DDL scripts to drop sequence
const templDropSeq94 = `DROP SEQUENCE {{.Name}};`
const templDropSeq95 = `DROP SEQUENCE IF EXISTS {{.Name}};`

var parsedTemplDropSeq = template.Must(template.New("drop_seq").Funcs(ddlFuncs).Parse(templDropSeq94))

func (s *Seq) dropScript() (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplDropSeq.Execute(&buffer, s); e != nil {
		return nil, &DDLError{table: s.Name, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: fmt.Sprintf("drop_seq#%s", s.Name), Code: buffer.String()}, nil
}

//Creates a full DDL to create a table and foreign keys refer to it
func (md *MetaDDL) CreateScript() (DDLStmts, error) {
	var stmts = DDLStmts{}
	for i, _ := range md.Seqs {
		if s, e := md.Seqs[i].createScript(); e != nil {
			return nil, e
		} else {
			stmts.Add(s)
		}
	}
	if s, e := md.createTableScript(); e != nil {
		return nil, e
	} else {
		stmts.Add(s)
	}
	return stmts, nil
}

//Creates a full DDL to remove a table and foreign keys refer to it
func (md *MetaDDL) DropScript() (DDLStmts, error) {
	var stmts = DDLStmts{}
	if s, e := md.dropTableScript(); e != nil {
		return nil, e
	} else {
		stmts.Add(s)
	}

	for i, _ := range md.Seqs {
		if s, e := md.Seqs[i].dropScript(); e != nil {
			return nil, e
		} else {
			stmts.Add(s)
		}
	}
	return stmts, nil
}

// Processor to copy elemnts from first slice to second one.
type SliceCopyProcessor interface {
	Id(int) string
	Len() int
	Copy(f int)
}

// Calculates two differences, m1 / m2 and m2 / m1, m1 and m2 is treated as sets.
// Id() is used as unique identifier in a set.
// Second slice of m1 contains m1 / m2 difference, second slice of m2 contains m2 / m1 difference.
func InverseIntersect(m1, m2 SliceCopyProcessor) {
	var set map[string]int = make(map[string]int)
	for i := 0; i < m1.Len(); i++ {
		set[m1.Id(i)] = i
	}
	for i := 0; i < m2.Len(); i++ {
		if _, e := set[m2.Id(i)]; e {
			delete(set, m2.Id(i))
		} else {
			m2.Copy(i)
		}
	}
	for _, v := range set {
		m1.Copy(v)
	}
}

type ColumnSliceCP struct {
	from []Column
	to   *[]Column
}

func (cp *ColumnSliceCP) Id(i int) string { return cp.from[i].Name }
func (cp *ColumnSliceCP) Len() int        { return len(cp.from) }
func (cp *ColumnSliceCP) Copy(i int)      { *cp.to = append(*cp.to, cp.from[i]) }

type SeqSliceCP struct {
	from []Seq
	to   *[]Seq
}

func (cp *SeqSliceCP) Id(i int) string { return cp.from[i].Name }
func (cp *SeqSliceCP) Len() int        { return len(cp.from) }
func (cp *SeqSliceCP) Copy(i int)      { *cp.to = append(*cp.to, cp.from[i]) }

type IFKSliceCP struct {
	from []IFK
	to   *[]IFK
}

func (cp *IFKSliceCP) Id(i int) string {
	return cp.from[i].FromColumn + cp.from[i].ToTable + cp.from[i].ToColumn
}
func (cp *IFKSliceCP) Len() int   { return len(cp.from) }
func (cp *IFKSliceCP) Copy(i int) { *cp.to = append(*cp.to, cp.from[i]) }

type OFKSliceCP struct {
	from []OFK
	to   *[]OFK
}

func (cp *OFKSliceCP) Id(i int) string {
	return cp.from[i].FromColumn + cp.from[i].ToTable + cp.from[i].ToColumn
}
func (cp *OFKSliceCP) Len() int   { return len(cp.from) }
func (cp *OFKSliceCP) Copy(i int) { *cp.to = append(*cp.to, cp.from[i]) }

// Difference of two meta DDL
type MetaDDLDiff struct {
	Table   string
	ColsRem []Column
	ColsAdd []Column
	IFKsRem []IFK
	IFKsAdd []IFK
	OFKsRem []OFK
	OFKsAdd []OFK
	SeqsAdd []Seq
	SeqsRem []Seq
}

// Calculate difference between two meta DDL
func (m1 *MetaDDL) Diff(m2 *MetaDDL) (*MetaDDLDiff, error) {
	if m1.Table != m2.Table {
		return nil, &DDLError{table: m1.Table, code: ErrInternal, msg: fmt.Sprintf("not the same tables are passed ot Diff: table1=%s, table2=%s", m1.Table, m2.Table)}
	}
	var mdd = &MetaDDLDiff{Table: m1.Table}
	InverseIntersect(&ColumnSliceCP{m1.Columns, &mdd.ColsRem}, &ColumnSliceCP{m2.Columns, &mdd.ColsAdd})
	InverseIntersect(&IFKSliceCP{m1.IFKs, &mdd.IFKsRem}, &IFKSliceCP{m2.IFKs, &mdd.IFKsAdd})
	InverseIntersect(&OFKSliceCP{m1.OFKs, &mdd.OFKsRem}, &OFKSliceCP{m2.OFKs, &mdd.OFKsAdd})
	InverseIntersect(&SeqSliceCP{m1.Seqs, &mdd.SeqsRem}, &SeqSliceCP{m2.Seqs, &mdd.SeqsAdd})
	return mdd, nil
}

func (m *MetaDDLDiff) Script() (DDLStmts, error) {
	var stmts = DDLStmts{}
	for i, _ := range m.IFKsRem {
		if s, e := m.IFKsRem[i].dropScript(m.Table); e != nil {
			return nil, e
		} else {
			stmts.Add(s)
		}
	}
	for i, _ := range m.ColsRem {
		if s, e := m.ColsRem[i].dropScript(m.Table); e != nil {
			return nil, e
		} else {
			stmts.Add(s)
		}
	}
	for i, _ := range m.SeqsRem {
		if s, e := m.SeqsRem[i].dropScript(); e != nil {
			return nil, e
		} else {
			stmts.Add(s)
		}
	}
	for i, _ := range m.SeqsAdd {
		if s, e := m.SeqsAdd[i].createScript(); e != nil {
			return nil, e
		} else {
			stmts.Add(s)
		}
	}
	for i, _ := range m.ColsAdd {
		if s, e := m.ColsAdd[i].addScript(m.Table); e != nil {
			return nil, e
		} else {
			stmts.Add(s)
		}
	}
	for i, _ := range m.IFKsAdd {
		if s, e := m.IFKsAdd[i].addScript(m.Table); e != nil {
			return nil, e
		} else {
			stmts.Add(s)
		}
	}
	/*for i, _ := range m.OFKsAdd {
		if s, e := m.OFKsAdd[i].createScript(); e != nil {
			return nil, e
		} else {
			stmts.Add(s)
		}
	}*/
	return stmts, nil
}

const TableNamePrefix string = "o_"

func tblName(m *meta.Meta) string {
	name := bytes.NewBufferString(TableNamePrefix)
	name.WriteString(m.Name)
	return name.String()
}

func tblAlias(m *meta.Meta) string {
	return string(m.Name[0])
}

func MetaDDLFromMeta(m *meta.Meta) (*MetaDDL, error) {
	var metaDdl *MetaDDL = &MetaDDL{Table: tblName(m), Pk: m.Key.Name}
	metaDdl.Columns = make([]Column, 0, len(m.Fields))
	metaDdl.IFKs = make([]IFK, 0, len(m.Fields)>>1)
	metaDdl.OFKs = make([]OFK, 0, len(m.Fields)>>1)
	metaDdl.Seqs = make([]Seq, 0)
	for i, _ := range m.Fields {
		f := &m.Fields[i]
		c := Column{}
		c.Name = f.Name
		c.Optional = f.Optional
		c.Unique = false
		colDef, err := newColDefVal(f)
		if err != nil {
			return nil, err
		}
		if c.Defval, err = colDef.ddlVal(); err != nil {
			return nil, err
		}
		if ds, ok := colDef.(*ColDefValSeq); ok {
			metaDdl.Seqs = append(metaDdl.Seqs, *ds.seq)
		}

		if f.IsSimple() {
			var ok bool
			if c.Typ, ok = fieldTypeToColumnType(f.Type); !ok {
				return nil, &DDLError{table: metaDdl.Table, code: ErrUnsupportedColumnType, msg: "Unsupported field type: " + string(f.Type)}
			}
			metaDdl.Columns = append(metaDdl.Columns, c)
		} else if f.Type == meta.FieldTypeObject && f.LinkType == meta.LinkTypeInner {
			var ok bool
			if c.Typ, ok = fieldTypeToColumnType(f.LinkMeta.Key.Type); !ok {
				return nil, &DDLError{table: metaDdl.Table, code: ErrUnsupportedColumnType, msg: "Unsupported field type: " + string(f.LinkMeta.Key.Type)}
			}
			metaDdl.Columns = append(metaDdl.Columns, c)
			metaDdl.IFKs = append(metaDdl.IFKs, IFK{FromColumn: f.Name, ToTable: tblName(f.LinkMeta), ToColumn: f.LinkMeta.Key.Name})
		} else if f.LinkType == meta.LinkTypeOuter {
			metaDdl.OFKs = append(metaDdl.OFKs, OFK{FromTable: tblName(f.LinkMeta), FromColumn: f.OuterLinkField.Name, ToTable: tblName(m), ToColumn: m.Key.Name})
		} else {
			return nil, &DDLError{table: metaDdl.Table, code: ErrUnsupportedLinkType, msg: fmt.Sprintf("Unsupported link type lt = %v, ft = %v", string(f.LinkType), string(f.LinkType))}
		}
	}
	return metaDdl, nil
}

var seqNameParseRe *regexp.Regexp = regexp.MustCompile("nextval\\('(.*)'::regclass\\)")

func MetaDDLFromDB(db *sql.DB, name string) (*MetaDDL, error) {
	md := &MetaDDL{Table: TableNamePrefix + name}
	reverser, err := NewReverser(db, md.Table)
	if err != nil {
		return nil, err
	}
	if err = reverser.Columns(&md.Columns, &md.Pk); err != nil {
		return nil, err
	}
	if err = reverser.Constraints(&md.IFKs, &md.OFKs); err != nil {
		return nil, err
	}

	for i, _ := range md.Columns {
		if strs := seqNameParseRe.FindAllStringSubmatch(md.Columns[i].Defval, -1); len(strs) > 0 {
			md.Seqs = append(md.Seqs, Seq{strs[0][1]})
		}
	}

	return md, nil
}

func (mddl *MetaDDL) create() {
	//	var b bytes.Buffer
	//	b.WriteString(")
}
