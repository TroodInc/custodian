package pg

import (
	"bytes"
	"custodian/server/object/description"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"text/template"
)

//DDL statament description
type DDLStmt struct {
	Name string
	Code string
	err  error
	time int
}

func NewDdlStatement(name string, code string) *DDLStmt {
	return &DDLStmt{Name: name, Code: code}
}

//Collection of the DDL statements
type DdlStatementSet []*DDLStmt

//Adds a DDL statement to the colletcion of them
func (ds *DdlStatementSet) Add(s *DDLStmt) {
	*ds = append(*ds, s)
}

func dbTypeToFieldType(dt string) (description.FieldType, bool) {
	if strings.HasPrefix(dt, "o_") {
		return description.FieldTypeEnum, true
	}
	switch dt {
	case "text":
		return description.FieldTypeString, true
	case "numeric":
		return description.FieldTypeNumber, true
	case "boolean":
		return description.FieldTypeBool, true
	case "timestamp with time zone":
		return description.FieldTypeDateTime, true
	case "date":
		return description.FieldTypeDate, true
	case "time with time zone":
		return description.FieldTypeTime, true
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
	Typ      description.FieldType
	Optional bool
	Unique   bool
	Defval   string
	Enum     description.EnumChoices
}

type IFK struct {
	FromColumn string
	ToTable    string
	ToColumn   string
	OnDelete   string
	Default    string
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

type ColDefDate struct{}

func (colDefDate *ColDefDate) ddlVal() (string, error) {
	return "CURRENT_DATE", nil
}

type ColDefTimestamp struct{}

func (colDefTimestamp *ColDefTimestamp) ddlVal() (string, error) {
	return "CURRENT_TIMESTAMP", nil
}

type ColDefNow struct{}

func (colDefNow *ColDefNow) ddlVal() (string, error) {
	return "NOW()", nil
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
	*description.DefExpr
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
		if len(v) > 0 {
			return fmt.Sprintf(`'%s'`, v), nil
		} else {
			return "", nil
		}
	case int:
		return strconv.Itoa(v), nil
	case uint:
		return strconv.Itoa(int(v)), nil
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

func newFieldSeq(metaName string, f *description.Field, args []interface{}) (*Seq, error) {
	if len(args) > 0 {
		if name, err := valToDdl(args[0]); err == nil {
			return &Seq{Name: name}, nil
		} else {
			return nil, err
		}
	} else {
		return &Seq{Name: GetTableName(metaName) + "_" + f.Name + "_seq"}, nil
	}
}

func defaultNextval(metaName string, f *description.Field, args []interface{}) (ColDefVal, error) {
	if s, err := newFieldSeq(metaName, f, args); err == nil {
		return &ColDefValSeq{s}, nil
	} else {
		return nil, err
	}
}

func defaultCurrentDate(metaName string, f *description.Field, args []interface{}) (ColDefVal, error) {
	return &ColDefDate{}, nil
}

func defaultCurrentTimestamp(metaName string, f *description.Field, args []interface{}) (ColDefVal, error) {
	return &ColDefTimestamp{}, nil
}

func defaultNow(metaName string, f *description.Field, args []interface{}) (ColDefVal, error) {
	return &ColDefNow{}, nil
}

func defaultOwner(metaName string, f *description.Field, args []interface{}) (ColDefVal, error) {
	return &ColDefValSimple{"0"}, nil
}

var defaultFuncs = map[string]func(metaName string, f *description.Field, args []interface{}) (ColDefVal, error){
	"nextval":           defaultNextval,
	"current_date":      defaultCurrentDate,
	"current_timestamp": defaultCurrentTimestamp,
	"now":               defaultNow,
	"owner":             defaultOwner,
}

func newColDefVal(metaName string, f *description.Field) (ColDefVal, error) {
	if def := f.Default(); def != nil {
		switch v := def.(type) {
		case description.DefConstStr:
			return newColDefValSimple(v.Value)
		case description.DefConstFloat:
			return newColDefValSimple(v.Value)
		case description.DefConstInt:
			return newColDefValSimple(v.Value)
		case description.DefConstBool:
			return newColDefValSimple(v.Value)
		case description.DefExpr:
			if fn, ok := defaultFuncs[strings.ToLower(v.Func)]; ok {
				return fn(metaName, f, v.Args)
			} else {
				return &ColDefValFunc{&v}, nil
			}
		default:
			return nil, &DDLError{code: ErrWrongDefultValue, msg: "Wrong default value"}
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
	templCreateTable = `CREATE TABLE "{{.Table}}" (
	{{$mtable:=.Table}}

	{{range .Columns}}
		{{template "column" dict "Mtable" $mtable "dot" .}},{{"\n"}}
	{{end}}

	{{range .IFKs}}
		{{template "ifk" dict "Mtable" $mtable "dot" .}},{{"\n"}}
	{{end}}
	
	PRIMARY KEY ("{{.Pk}}")
    );`
	templCreateTableColumns = `{{define "column"}}
		{{ $enum := len .dot.Enum}}
		
		"{{.dot.Name}}" 
		{{ if gt $enum 0 }} {{ .Mtable }}_{{ .dot.Name }} {{ else }} {{ .dot.Typ.DdlType }}{{ end }}
		{{if not .dot.Optional}} NOT NULL{{end}}{{if .dot.Unique}} UNIQUE{{end}}
		{{if .dot.Defval}} DEFAULT {{.dot.Defval}}{{end}}{{end}}`
	templCreateTableInnerFK = `{{define "ifk"}}
		CONSTRAINT fk_{{.dot.FromColumn}}_{{.dot.ToTable}}_{{.dot.ToColumn}} 
		FOREIGN KEY ("{{.dot.FromColumn}}") 
		REFERENCES "{{.dot.ToTable}}" ("{{.dot.ToColumn}}") 
		ON DELETE {{.dot.OnDelete}} 
			{{if eq .dot.OnDelete "SET DEFAULT" }} {{ .dot.Default }} {{end}}
		{{end}}`
)

var parsedTemplCreateTable = template.Must(template.Must(template.Must(template.New("create_table_ddl").Funcs(ddlFuncs).Parse(templCreateTable)).Parse(templCreateTableColumns)).Parse(templCreateTableInnerFK))

//Creates a DDL script to make a table based on the metadata
func (md *MetaDDL) CreateTableDdlStatement() (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplCreateTable.Execute(&buffer, md); e != nil {
		return nil, &DDLError{table: md.Table, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: "create_table#" + md.Table, Code: buffer.String()}, nil
}

//DDL drop table template
const templDropTable94 = `DROP TABLE "{{.Table}}" {{.Mode}};`

var parsedTemplDropTable = template.Must(template.New("drop_table").Funcs(ddlFuncs).Parse(templDropTable94))

//Creates a DDL to drop a table
func (md *MetaDDL) DropTableDdlStatement(force bool) (*DDLStmt, error) {
	var buffer bytes.Buffer
	var mode string
	if force {
		mode = "CASCADE"
	} else {
		mode = "RESTRICT"
	}
	if e := parsedTemplDropTable.Execute(&buffer, map[string]string{"Table": md.Table, "Mode": mode}); e != nil {
		return nil, &DDLError{table: md.Table, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: "drop_table#" + md.Table, Code: buffer.String()}, nil
}

//DDL drop table column template
const templDropTableColumn = `ALTER TABLE "{{.Table}}" DROP COLUMN "{{.dot.Name}}";`

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
const templAddTableColumn = `ALTER TABLE "{{.Table}}" ADD COLUMN "{{.dot.Name}}" {{.dot.Typ.DdlType}}{{if not .dot.Optional}} NOT NULL{{end}}{{if .dot.Unique}} UNIQUE{{end}}{{if .dot.Defval}} DEFAULT {{.dot.Defval}}{{end}};`

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

const templAlterTableColumnAlterType = `ALTER TABLE "{{.Table}}" ALTER COLUMN "{{.dot.Name}}" SET DATA TYPE {{.dot.Typ.DdlType}};`
const templAlterTableColumnAlterNull = `ALTER TABLE "{{.Table}}" ALTER COLUMN "{{.dot.Name}}" {{if not .dot.Optional}} SET {{else}} DROP {{end}} NOT NULL;`
const templAlterTableColumnAlterDefault = `ALTER TABLE "{{.Table}}" ALTER COLUMN "{{.dot.Name}}" {{if .dot.Defval}} SET DEFAULT {{.dot.Defval}} {{else}} DROP DEFAULT {{end}};`

var parsedTemplAlterTableColumnAlterType = template.Must(template.New("alter_table_column_alter_type").Funcs(ddlFuncs).Parse(templAlterTableColumnAlterType))
var parsedTemplAlterTableColumnAlterNull = template.Must(template.New("alter_table_column_alter_null").Funcs(ddlFuncs).Parse(templAlterTableColumnAlterNull))
var parsedTemplAlterTableColumnAlterDefault = template.Must(template.New("alter_table_column_alter_default").Funcs(ddlFuncs).Parse(templAlterTableColumnAlterDefault))

//Creates a DDL to alter a table's column
func (cl *Column) alterScript(tname string) (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplAlterTableColumnAlterType.Execute(&buffer, map[string]interface{}{
		"Table": tname,
		"dot":   cl}); e != nil {
		return nil, &DDLError{table: tname, code: ErrInternal, msg: e.Error()}
	}
	if e := parsedTemplAlterTableColumnAlterNull.Execute(&buffer, map[string]interface{}{
		"Table": tname,
		"dot":   cl}); e != nil {
		return nil, &DDLError{table: tname, code: ErrInternal, msg: e.Error()}
	}
	if e := parsedTemplAlterTableColumnAlterDefault.Execute(&buffer, map[string]interface{}{
		"Table": tname,
		"dot":   cl}); e != nil {
		return nil, &DDLError{table: tname, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: fmt.Sprintf("alter_table_column#%s.%s", tname, cl.Name), Code: buffer.String()}, nil
}

//DDL create table outer foreign key templates
const templCreateOuterFK = `ALTER TABLE "{{.FromTable}}" ADD CONSTRAINT fk_{{.FromColumn}}_{{.ToTable}}_{{.ToColumn}} FOREIGN KEY "({{.FromColumn}})" REFERENCES "{{.ToTable}}" ("{{.ToColumn}}");`

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
const templDropOuterFK = `ALTER TABLE "{{.FromTable}}" DROP CONSTRAINT fk_{{.FromColumn}}_{{.ToTable}}_{{.ToColumn}};`

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
const templDropInnerFK = `ALTER TABLE "{{.Table}}" DROP CONSTRAINT fk_{{.dot.FromColumn}}_{{.dot.ToTable}}_{{.dot.ToColumn}};`

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
const templAddInnerFK = `ALTER TABLE "{{.Table}}" ADD CONSTRAINT fk_{{.dot.FromColumn}}_{{.dot.ToTable}}_{{.dot.ToColumn}} FOREIGN KEY ("{{.dot.FromColumn}}") REFERENCES "{{.dot.ToTable}}" ("{{.dot.ToColumn}}");`

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
const templCreateSeq94 = `CREATE SEQUENCE "{{.Name}}";`

var parsedTemplCreateSeq = template.Must(template.New("add_seq").Funcs(ddlFuncs).Parse(templCreateSeq94))

func (s *Seq) CreateDdlStatement() (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplCreateSeq.Execute(&buffer, s); e != nil {
		return nil, &DDLError{table: s.Name, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: fmt.Sprintf("create_seq#%s", s.Name), Code: buffer.String()}, nil
}

//DDL scripts to drop sequence
const templDropSeq94 = `DROP SEQUENCE "{{.Name}}";`

var parsedTemplDropSeq = template.Must(template.New("drop_seq").Funcs(ddlFuncs).Parse(templDropSeq94))

func (s *Seq) DropDdlStatement() (*DDLStmt, error) {
	var buffer bytes.Buffer
	if e := parsedTemplDropSeq.Execute(&buffer, s); e != nil {
		return nil, &DDLError{table: s.Name, code: ErrInternal, msg: e.Error()}
	}
	return &DDLStmt{Name: fmt.Sprintf("drop_seq#%s", s.Name), Code: buffer.String()}, nil
}

//Creates a full DDL to create a table and foreign getColumnsToInsert refer to it
func (md *MetaDDL) CreateScript() (DdlStatementSet, error) {
	var stmts = DdlStatementSet{}
	for i, _ := range md.Seqs {
		if statement, err := md.Seqs[i].CreateDdlStatement(); err != nil {
			return nil, err
		} else {
			stmts.Add(statement)
		}
	}
	if s, e := md.CreateTableDdlStatement(); e != nil {
		return nil, e
	} else {
		stmts.Add(s)
	}
	return stmts, nil
}

func (c *Column) GetEnumStatement(table string) (*DDLStmt, error) {
	QuotedChoices(c.Enum)
	statement, err := CreateEnumStatement(table, c.Name, c.Enum)
	if err != nil {
		return nil, err
	}
	return statement, nil
}

//Creates a full DDL to remove a table and foreign getColumnsToInsert refer to it
func (md *MetaDDL) DropScript(force bool) (DdlStatementSet, error) {
	var stmts = DdlStatementSet{}
	if s, e := md.DropTableDdlStatement(force); e != nil {
		return nil, e
	} else {
		stmts.Add(s)
	}

	for i, _ := range md.Seqs {
		if s, e := md.Seqs[i].DropDdlStatement(); e != nil {
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
// Data slice of m1 contains m1 / m2 difference, second slice of m2 contains m2 / m1 difference.
func InverseIntersect(m1, m2 SliceCopyProcessor) {
	var set = make(map[string]int)
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
	Table     string
	ColsRem   []Column
	ColsAdd   []Column
	ColsAlter []Column
	IFKsRem   []IFK
	IFKsAdd   []IFK
	OFKsRem   []OFK
	OFKsAdd   []OFK
	SeqsAdd   []Seq
	SeqsRem   []Seq
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
	//process fields update check
	for _, currentObjectColumn := range m1.Columns {
		for _, objectToUpdateColumn := range m2.Columns {
			//omit this check for PK`s until TB-116 is implemented
			if currentObjectColumn.Name == objectToUpdateColumn.Name && m1.Pk != currentObjectColumn.Name {
				if currentObjectColumn.Optional != objectToUpdateColumn.Optional ||
					currentObjectColumn.Typ != objectToUpdateColumn.Typ {
					mdd.ColsAlter = append(mdd.ColsAlter, objectToUpdateColumn)
				}
			}
		}
	}
	return mdd, nil
}

func (m *MetaDDLDiff) Script() (DdlStatementSet, error) {
	var stmts = DdlStatementSet{}
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
		if s, e := m.SeqsRem[i].DropDdlStatement(); e != nil {
			return nil, e
		} else {
			stmts.Add(s)
		}
	}
	for i, _ := range m.SeqsAdd {
		if s, e := m.SeqsAdd[i].CreateDdlStatement(); e != nil {
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
	for i, _ := range m.ColsAlter {
		if s, e := m.ColsAlter[i].alterScript(m.Table); e != nil {
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
		if s, e := m.OFKsAdd[i].CreateTableDdlStatement(); e != nil {
			return nil, e
		} else {
			stmts.Add(s)
		}
	}*/
	return stmts, nil
}

const TableNamePrefix = "o_"

func GetTableName(metaName string) string {
	name := bytes.NewBufferString(TableNamePrefix)
	name.WriteString(metaName)
	return name.String()
}

var seqNameParseRe = regexp.MustCompile("nextval\\('(?:\"?)(.*?)(?:\"?)'::regclass\\)")

func MetaDDLFromDB(tx *sql.Tx, name string) (*MetaDDL, error) {
	md := &MetaDDL{Table: TableNamePrefix + name}
	reverser, err := NewReverser(tx, md.Table)
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
