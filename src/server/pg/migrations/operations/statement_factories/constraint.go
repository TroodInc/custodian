package statement_factories

import (
	"server/pg"
	"bytes"
	"fmt"
	"text/template"
)

type ConstraintStatementFactory struct{}

func (ssm *ConstraintStatementFactory) FactoryCreateStatement(tableName string, ifk *pg.IFK) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	context := map[string]interface{}{"Table": tableName, "Ifk": ifk}
	if e := parsedAddInnerFkConstraintTemplate.Execute(&buffer, context); e != nil {
		return nil, pg.NewDdlError(pg.ErrInternal, e.Error(), tableName)
	}
	return pg.NewDdlStatement(fmt.Sprintf("create_ifk#%s", tableName), buffer.String()), nil
}

func (ssm *ConstraintStatementFactory) FactoryDropStatement(tableName string, ifk *pg.IFK) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	context := map[string]interface{}{"Table": tableName, "Ifk": ifk}
	if e := parsedDropInnerFkConstraintTemplate.Execute(&buffer, context); e != nil {
		return nil, pg.NewDdlError(pg.ErrInternal, e.Error(), tableName)
	}
	return pg.NewDdlStatement(fmt.Sprintf("drop_ifk#%s", tableName), buffer.String()), nil
}

//Templates
const addInnerFkConstraintTemplate = `
	ALTER TABLE "{{.Table}}" 
	ADD CONSTRAINT fk_{{.Ifk.FromColumn}}_{{.Ifk.ToTable}}_{{.Ifk.ToColumn}} 
	FOREIGN KEY ("{{.Ifk.FromColumn}}") 
	REFERENCES "{{.Ifk.ToTable}}" ("{{.Ifk.ToColumn}}") 
	ON DELETE {{.Ifk.OnDelete}} 
	{{if eq .Ifk.OnDelete "SET DEFAULT" }} 
		{{ .Ifk.Default }} 
	{{end}};
`

var parsedAddInnerFkConstraintTemplate = template.Must(template.New("add_constraint").Parse(addInnerFkConstraintTemplate))

//Templates
const dropInnerFkConstraintTemplate = `
	ALTER TABLE "{{.Table}}" 
	DROP CONSTRAINT fk_{{.Ifk.FromColumn}}_{{.Ifk.ToTable}}_{{.Ifk.ToColumn}}
`

var parsedDropInnerFkConstraintTemplate = template.Must(template.New("drop_constraint").Parse(dropInnerFkConstraintTemplate))
