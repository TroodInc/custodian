package statement_factories

import (
	"bytes"
	"custodian/server/object"
	"fmt"
	"text/template"
)

type TableStatementFactory struct{}

func (csm *TableStatementFactory) FactoryRenameStatement(tableName string, newName string) (*object.DDLStmt, error) {
	var buffer bytes.Buffer
	context := map[string]interface{}{"Table": tableName, "NewName": newName}
	if e := parsedRenameTableTemplate.Execute(&buffer, context); e != nil {
		return nil, object.NewDdlError(object.ErrInternal, e.Error(), tableName)
	}
	return object.NewDdlStatement(fmt.Sprintf("add_column#%s", tableName), buffer.String()), nil
}

//Templates

const renameTableTemplate = `ALTER TABLE "{{.Table}}" RENAME TO {{.NewName}};`

var parsedRenameTableTemplate = template.Must(template.New("rename_table").Parse(renameTableTemplate))
