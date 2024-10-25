package statement_factories

import (
	"bytes"
	"custodian/server/object"
	"custodian/server/object/description"
	"fmt"
	"text/template"
)

type ColumnStatementFactory struct{}

var statementsMap = map[string]string{
	"add_enum_column":           `ALTER TABLE "{{.Table}}" ADD COLUMN "{{.Column.Name}}" "{{.Table}}_{{.Column.Name}}";`,
	"add_column":                `ALTER TABLE "{{.Table}}" ADD COLUMN "{{.Column.Name}}" {{.Column.Typ.DdlType}} {{if not .Column.Optional}} NOT NULL{{end}} {{if .Column.Unique}} UNIQUE{{end}} {{if .Column.Defval}} DEFAULT {{.Column.Defval}}{{end}};`,
	"drop_column":               `ALTER TABLE "{{.Table}}" DROP COLUMN "{{.Column.Name}}";`,
	"rename_column":             `ALTER TABLE "{{.Table}}" RENAME "{{.CurrentName}}" TO "{{.NewName}}";`,
	"alter_column_set_null":     `ALTER TABLE "{{.Table}}" ALTER COLUMN "{{.Column.Name}}" {{if not .Column.Optional}} SET {{else}} DROP {{end}} NOT NULL;`,
	"alter_column_set_default":  `ALTER TABLE "{{.Table}}" ALTER COLUMN "{{.Column.Name}}" {{if .Column.Defval}} SET DEFAULT {{.Column.Defval}}{{if eq .Column.Typ .FieldTypeEnum}}::"{{.Table}}_{{.Column.Name}}"{{end}}{{else}} DROP DEFAULT {{end}};`,
	"alter_column_drop_default": `ALTER TABLE "{{.Table}}" ALTER COLUMN "{{.Column.Name}}" DROP DEFAULT;`,
	"alter_column_set_type":     `{{$enum := len .Column.Enum}} ALTER TABLE "{{.Table}}" ALTER COLUMN "{{.Column.Name}}" SET DATA TYPE {{ if gt $enum 0 }} "{{.Table}}_{{.Column.Name}}" USING ("{{.Column.Name}}"::text::"{{.Table}}_{{.Column.Name}})" {{else}} {{.Column.Typ.DdlType}} {{end}};`,
}

func (csm *ColumnStatementFactory) build(statement string, tableName string, context map[string]interface{}) (*object.DDLStmt, error) {
	var buffer bytes.Buffer
	context["Table"] = tableName
	statementTemplate := template.Must(template.New("statement").Parse(statementsMap[statement]))
	if e := statementTemplate.Execute(&buffer, context); e != nil {
		return nil, object.NewDdlError(object.ErrInternal, e.Error(), tableName)
	}
	return object.NewDdlStatement(fmt.Sprintf("%s#%s", statement, tableName), buffer.String()), nil
}

func (csm *ColumnStatementFactory) FactoryRenameStatement(tableName string, currentColumn object.Column, newColumn object.Column) (*object.DDLStmt, error) {
	context := map[string]interface{}{"CurrentName": currentColumn.Name, "NewName": newColumn.Name}
	return csm.build("rename_column", tableName, context)
}

func (csm *ColumnStatementFactory) FactorySetNullStatement(tableName string, Column object.Column) (*object.DDLStmt, error) {
	context := map[string]interface{}{"Column": Column}
	return csm.build("alter_column_set_null", tableName, context)
}

func (csm *ColumnStatementFactory) FactoryDropDefaultStatement(tableName string, Column object.Column) (*object.DDLStmt, error) {
	context := map[string]interface{}{"Column": Column}
	return csm.build("alter_column_drop_default", tableName, context)
}

func (csm *ColumnStatementFactory) FactorySetDefaultStatement(tableName string, Column object.Column) (*object.DDLStmt, error) {
	context := map[string]interface{}{"Column": Column, "FieldTypeEnum": description.FieldTypeEnum}
	return csm.build("alter_column_set_default", tableName, context)
}

func (csm *ColumnStatementFactory) FactorySetTypeStatement(tableName string, Column object.Column) (*object.DDLStmt, error) {
	context := map[string]interface{}{"Column": Column}
	return csm.build("alter_column_set_type", tableName, context)
}

func (csm *ColumnStatementFactory) FactoryAddStatement(tableName string, Column object.Column) (*object.DDLStmt, error) {
	context := map[string]interface{}{"Column": Column}
	return csm.build("add_column", tableName, context)
}

func (csm *ColumnStatementFactory) FactoryDropStatement(tableName string, Column object.Column) (*object.DDLStmt, error) {
	context := map[string]interface{}{"Column": Column}
	return csm.build("drop_column", tableName, context)
}

func (csm *ColumnStatementFactory) FactoryAddEnumStatement(tableName string, Column object.Column) (*object.DDLStmt, error) {
	context := map[string]interface{}{"Column": Column}
	return csm.build("add_enum_column", tableName, context)
}
