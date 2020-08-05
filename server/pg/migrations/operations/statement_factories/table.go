package statement_factories

import (
	"server/pg"
	"bytes"
	"fmt"
	"text/template"
)

type TableStatementFactory struct{}

func (csm *TableStatementFactory) FactoryRenameStatement(tableName string, newName string) (*pg.DDLStmt, error) {
	var buffer bytes.Buffer
	context := map[string]interface{}{"Table": tableName, "NewName": newName}
	if e := parsedRenameTableTemplate.Execute(&buffer, context); e != nil {
		return nil, pg.NewDdlError(pg.ErrInternal, e.Error(), tableName)
	}
	return pg.NewDdlStatement(fmt.Sprintf("add_column#%s", tableName), buffer.String()), nil
}

//Templates

const renameTableTemplate = `ALTER TABLE "{{.Table}}" RENAME TO {{.NewName}};`

var parsedRenameTableTemplate = template.Must(template.New("rename_table").Parse(renameTableTemplate))
