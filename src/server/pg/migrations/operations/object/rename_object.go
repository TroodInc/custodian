package object

import (
	"server/migrations/operations/object"
	"server/object/meta"
	"database/sql"
	"server/pg"
	"logger"
	"server/transactions"
	"fmt"
	"text/template"
	"bytes"
)

type RenameObjectOperation struct {
	object.RenameObjectOperation
}

func (o *RenameObjectOperation) SyncDbDescription(metaObj *meta.Meta, transaction transactions.DbTransaction) (err error) {
	tx := transaction.Transaction().(*sql.Tx)

	//rename table
	var buffer bytes.Buffer
	tableName := pg.GetTableName(metaObj.Name)
	newTableName := pg.GetTableName(o.NewName)
	if e := parsedRenameTableTemplate.Execute(&buffer, map[string]string{"Table": tableName, "NewName": newTableName}); e != nil {
		return pg.NewDdlError(tableName, pg.ErrInternal, e.Error())
	}
	statement := pg.DDLStmt{Name: "rename_table#" + tableName, Code: buffer.String()}

	logger.Debug("Renaming object in DB: %syncer\n", statement.Code)
	if _, err = tx.Exec(statement.Code); err != nil {
		return pg.NewDdlError(metaObj.Name, pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
	}
	return nil
}

const renameTableTemplate = `ALTER TABLE "{{.Table}}" RENAME TO {{.NewName}};`

var parsedRenameTableTemplate = template.Must(template.New("rename_table").Funcs(ddlFuncs).Parse(renameTableTemplate))

func NewRenameObjectOperation(newName string) *RenameObjectOperation {
	return &RenameObjectOperation{RenameObjectOperation: object.RenameObjectOperation{NewName: newName}}
}
