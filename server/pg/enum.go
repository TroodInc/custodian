package pg

import (
	"bytes"
	"fmt"

	"custodian/server/object/description"

	"strings"
	"text/template"
)

func CreateEnumStatement(tableName string, fieldName string, choices description.EnumChoices) (*DDLStmt, error) {
	const createEnumTemplate = `CREATE TYPE {{.Table}}_{{.Column}} AS ENUM ({{.Choices}});`
	var buffer bytes.Buffer

	enumChoices := strings.Join(choices[:], ", ")
	context := map[string]interface{}{"Table": tableName, "Column": fieldName, "Choices": enumChoices}
	parsedEnumTemplate := template.Must(template.New("statement").Parse(createEnumTemplate))
	if e := parsedEnumTemplate.Execute(&buffer, context); e != nil {
		return nil, NewDdlError(ErrInternal, e.Error(), tableName)
	}
	return NewDdlStatement(fmt.Sprintf("add_type#%s_%s ", tableName, fieldName), buffer.String()), nil
}

func DropEnumStatement(tableName string, fieldName string) (*DDLStmt, error) {
	const dropEnumTemplate = `DROP TYPE IF EXISTS {{.Table}}_{{.Column}};`
	var buffer bytes.Buffer

	context := map[string]interface{}{"Table": tableName, "Column": fieldName}
	parsedEnumTemplate := template.Must(template.New("statement").Parse(dropEnumTemplate))
	if e := parsedEnumTemplate.Execute(&buffer, context); e != nil {
		return nil, NewDdlError(ErrInternal, e.Error(), tableName)
	}
	return NewDdlStatement(fmt.Sprintf("drop_type#%s_%s ", tableName, fieldName), buffer.String()), nil
}

func QuotedChoices(choices description.EnumChoices)  {
	for idx, itm := range choices {
		choices[idx] = fmt.Sprintf("'%s'", itm)
	}
}