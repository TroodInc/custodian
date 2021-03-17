package object

import (
	"bytes"
	"fmt"

	"custodian/server/object/description"

	"text/template"
)

func CreateEnumStatement(tableName string, fieldName string, choices description.EnumChoices) (*DDLStmt, error) {
	const createEnumTemplate = `
	DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{{.Table}}_{{.Column}}') THEN
			CREATE TYPE "{{.Table}}_{{.Column}}" AS ENUM ({{.Choices}});
  		END IF;
	END$$;`
	var buffer bytes.Buffer
	choicesString := ""

	for idx, itm := range choices {
		choicesString = choicesString + " '" + itm + "' "
		if idx != len(choices) - 1 {
			choicesString = choicesString + ", "
		}
	}

	context := map[string]interface{}{"Table": tableName, "Column": fieldName, "Choices": choicesString}
	parsedEnumTemplate := template.Must(template.New("statement").Parse(createEnumTemplate))
	if e := parsedEnumTemplate.Execute(&buffer, context); e != nil {
		return nil, NewDdlError(ErrInternal, e.Error(), tableName)
	}
	return NewDdlStatement(fmt.Sprintf("add_type#%s_%s ", tableName, fieldName), buffer.String()), nil
}

func RenameEnumStatement(tableName string, oldColumn string, newColumn string) (*DDLStmt, error) {
	const renameEnumTemplate = `ALTER TYPE "{{.Table}}_{{.OldColumn}}" RENAME TO "{{.Table}}_{{.NewColumn}}";`
	var buffer bytes.Buffer
	context := map[string]interface{}{"Table": tableName, "OldColumn": oldColumn, "NewColumn": newColumn}
	parsedEnumTemplate := template.Must(template.New("statement").Parse(renameEnumTemplate))
	if e := parsedEnumTemplate.Execute(&buffer, context); e != nil {
		return nil, NewDdlError(ErrInternal, e.Error(), tableName)
	}
	return NewDdlStatement(fmt.Sprintf("rename_enum#%s_%s ", tableName, oldColumn ), buffer.String()), nil
}

func DropEnumStatement(tableName string, fieldName string) (*DDLStmt, error) {
	const dropEnumTemplate = `DROP TYPE IF EXISTS "{{.Table}}_{{.Column}}";`
	var buffer bytes.Buffer

	context := map[string]interface{}{"Table": tableName, "Column": fieldName}
	parsedEnumTemplate := template.Must(template.New("statement").Parse(dropEnumTemplate))
	if e := parsedEnumTemplate.Execute(&buffer, context); e != nil {
		return nil, NewDdlError(ErrInternal, e.Error(), tableName)
	}
	return NewDdlStatement(fmt.Sprintf("drop_type#%s_%s ", tableName, fieldName), buffer.String()), nil
}

func AddEnumStatement(tableName string, fieldName string, choice string) (*DDLStmt, error) {
	const addEnumTemplate = `ALTER TYPE "{{.Table}}_{{.Column}}" ADD VALUE IF NOT EXISTS '{{.Choice}}';`
	var buffer bytes.Buffer
	context := map[string]interface{}{"Table": tableName, "Column": fieldName, "Choice": choice}
	parsedEnumTemplate := template.Must(template.New("statement").Parse(addEnumTemplate))
	if e := parsedEnumTemplate.Execute(&buffer, context); e != nil {
		return nil, NewDdlError(ErrInternal, e.Error(), tableName)
	}
	return NewDdlStatement(fmt.Sprintf("add_enum#%s_%s ", tableName, fieldName), buffer.String()), nil
}

func ChoicesIsCompleting(oldChoices description.EnumChoices, newChoices description.EnumChoices ) bool {
	exists := false
	for _, oldChoice := range oldChoices {
		for _, newChoice := range newChoices{
			if oldChoice == newChoice {
				exists = true
				break
			}
		}
		if !exists {
			return false
		}
		exists = false
	}
	return true
}