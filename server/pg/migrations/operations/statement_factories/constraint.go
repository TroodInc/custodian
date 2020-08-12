package statement_factories

import (
	"custodian/server/pg"
	"bytes"
	"fmt"
	"text/template"
)

type ConstraintStatementFactory struct{}

var constraintsMap = map[string] string {
	"create_ifk": `ALTER TABLE "{{.Table}}" ADD CONSTRAINT fk_{{.Ifk.FromColumn}}_{{.Ifk.ToTable}}_{{.Ifk.ToColumn}} FOREIGN KEY ("{{.Ifk.FromColumn}}") REFERENCES "{{.Ifk.ToTable}}" ("{{.Ifk.ToColumn}}") ON DELETE {{.Ifk.OnDelete}} {{if eq .Ifk.OnDelete "SET DEFAULT" }} {{ .Ifk.Default }} {{end}};`,
	"drop_ifk": `ALTER TABLE "{{.Table}}" DROP CONSTRAINT fk_{{.Ifk.FromColumn}}_{{.Ifk.ToTable}}_{{.Ifk.ToColumn}}`,
	"set_unique": `ALTER TABLE "{{.Table}}" {{if not .Column.Unique }} DROP {{ else }} ADD {{ end }} CONSTRAINT {{.Table}}_{{.Column.Name}}_key {{if .Column.Unique }} UNIQUE ({{.Column.Name}}) {{ end }}`,
}

func (ssm *ConstraintStatementFactory) build(constraint string, tableName string, context map[string]interface{}) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	context["Table"] = tableName
	constraintTemplate := template.Must(template.New("add_constraint").Parse(constraintsMap[constraint]))
	if e := constraintTemplate.Execute(&buffer, context); e != nil {
		return nil, pg.NewDdlError(pg.ErrInternal, e.Error(), tableName)
	}
	return pg.NewDdlStatement(fmt.Sprintf("%s#%s", constraint, tableName), buffer.String()), nil
}

func (ssm *ConstraintStatementFactory) FactoryDropIFKStatement(tableName string, ifk *pg.IFK) (*pg.DDLStmt, error) {
	context := map[string]interface{}{"Table": tableName, "Ifk": ifk}
	return ssm.build("drop_ifk", tableName, context)
}

func (ssm *ConstraintStatementFactory) FactoryCreateIFKStatement(tableName string, ifk *pg.IFK) (*pg.DDLStmt, error) {
	context := map[string]interface{}{"Table": tableName, "Ifk": ifk}
	return ssm.build("create_ifk", tableName, context)
}

func (ssm *ConstraintStatementFactory) FactorySetUniqueStatement(tableName string, Column pg.Column) (*pg.DDLStmt, error) {
	context := map[string]interface{}{"Table": tableName, "Column": Column}
	return ssm.build("set_unique", tableName, context)
}
