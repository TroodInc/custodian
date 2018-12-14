package statement_factories

import (
	"server/pg"
	"bytes"
	"fmt"
	"text/template"
)

type ColumnStatementFactory struct{}

func (csm *ColumnStatementFactory) FactoryAddStatement(tableName string, column pg.Column) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	context := map[string]interface{}{"Table": tableName, "Column": column}
	if e := parsedAddTableColumnTemplate.Execute(&buffer, context); e != nil {
		return nil, pg.NewDdlError(pg.ErrInternal, e.Error(), tableName)
	}
	return pg.NewDdlStatement(fmt.Sprintf("add_column#%s", tableName), buffer.String()), nil
}

func (csm *ColumnStatementFactory) FactoryRenameStatement(tableName string, currentColumn pg.Column, newColumn pg.Column) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	context := map[string]string{"Table": tableName, "CurrentName": currentColumn.Name, "NewName": newColumn.Name}
	if e := parsedAlterColumnRenameTemplate.Execute(&buffer, context); e != nil {
		return nil, pg.NewDdlError(pg.ErrInternal, e.Error(), tableName)
	}
	return pg.NewDdlStatement(fmt.Sprintf("rename_column#%s", currentColumn.Name), buffer.String()), nil
}

func (csm *ColumnStatementFactory) FactorySetNullStatement(tableName string, column pg.Column) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	context := map[string]interface{}{"Table": tableName, "Column": column}
	if e := parsedAlterColumnSetNull.Execute(&buffer, context); e != nil {
		return nil, pg.NewDdlError(pg.ErrInternal, e.Error(), tableName)
	}
	return pg.NewDdlStatement(fmt.Sprintf("alter_column_set_null#%s", tableName), buffer.String()), nil
}

func (csm *ColumnStatementFactory) FactorySetDefaultStatement(tableName string, column pg.Column) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	context := map[string]interface{}{"Table": tableName, "Column": column}
	if e := parsedAlterColumnSetDefault.Execute(&buffer, context); e != nil {
		return nil, pg.NewDdlError(pg.ErrInternal, e.Error(), tableName)
	}
	return pg.NewDdlStatement(fmt.Sprintf("alter_column_set_default#%s", tableName), buffer.String()), nil
}

func (csm *ColumnStatementFactory) FactorySetTypeStatement(tableName string, column pg.Column) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	context := map[string]interface{}{"Table": tableName, "Column": column}
	if e := parsedAlterColumnSetType.Execute(&buffer, context); e != nil {
		return nil, pg.NewDdlError(pg.ErrInternal, e.Error(), tableName)
	}
	return pg.NewDdlStatement(fmt.Sprintf("alter_column_set_type#%s", tableName), buffer.String()), nil
}

//Templates
const alterColumnRenameTemplate = `ALTER TABLE "{{.Table}}" RENAME "{{.CurrentName}}" TO "{{.NewName}}";`

var parsedAlterColumnRenameTemplate = template.Must(template.New("alter_table_column_rename").Parse(alterColumnRenameTemplate))

const alterColumnSetNull = `ALTER TABLE "{{.Table}}" ALTER COLUMN "{{.Column.Name}}" {{if not .Column.Optional}} SET {{else}} DROP {{end}} NOT NULL;`

var parsedAlterColumnSetNull = template.Must(template.New("alter_table_column_set_null").Parse(alterColumnSetNull))

const alterColumnSetDefault = `ALTER TABLE "{{.Table}}" ALTER COLUMN "{{.Column.Name}}" {{if .Column.Defval}} SET DEFAULT {{.Column.Defval}} {{else}} DROP DEFAULT {{end}};`

var parsedAlterColumnSetDefault = template.Must(template.New("alter_table_column_set_default").Parse(alterColumnSetDefault))

const alterColumnSetType = `ALTER TABLE "{{.Table}}" ALTER COLUMN "{{.Column.Name}}" SET DATA TYPE {{.Column.Typ.DdlType}};`

var parsedAlterColumnSetType = template.Must(template.New("alter_table_column_set_type").Parse(alterColumnSetType))

const addTableColumnTemplate = `
	ALTER TABLE "{{.Table}}" 
	ADD COLUMN "{{.Column.Name}}" {{.Column.Typ.DdlType}}
	{{if not .Column.Optional}} NOT NULL{{end}}
	{{if .Column.Unique}} UNIQUE{{end}}
	{{if .Column.Defval}} DEFAULT {{.Column.Defval}}{{end}};`

var parsedAddTableColumnTemplate = template.Must(template.New("add_table_column").Parse(addTableColumnTemplate))
