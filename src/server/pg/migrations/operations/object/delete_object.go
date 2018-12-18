package object

import (
	"server/object/meta"
	"server/transactions"
	"server/migrations/operations/object"
	"database/sql"
	"server/pg"
	"logger"
	"fmt"
	"text/template"
)

type DeleteObjectOperation struct {
	object.DeleteObjectOperation
}

func (o *DeleteObjectOperation) SyncDbDescription(metaObj *meta.Meta, transaction transactions.DbTransaction) (err error) {
	tx := transaction.Transaction().(*sql.Tx)
	var metaDdl *pg.MetaDDL
	if metaDdl, err = new(pg.MetaDdlFactory).Factory(metaObj); err != nil {
		return err
	}

	var statementSet = pg.DdlStatementSet{}

	//remove table itself
	if statement, err := metaDdl.DropTableDdlStatement(false); err != nil {
		return err
	} else {
		statementSet.Add(statement)
	}

	//remove sequences
	for i, _ := range metaDdl.Seqs {
		if statement, err := metaDdl.Seqs[i].DropDdlStatement(); err != nil {
			return err
		} else {
			statementSet.Add(statement)
		}
	}

	for _, statement := range statementSet {
		logger.Debug("Removing object in DB: %syncer\n", statement.Code)
		if _, err = tx.Exec(statement.Code); err != nil {
			return pg.NewDdlError(metaObj.Name, pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
		}
	}
	return nil
}

const dropTableTemplate = `DROP TABLE "{{.Table}}" {{.Mode}};`

var parsedDropTableTemplate = template.Must(template.New("drop_table").Funcs(ddlFuncs).Parse(dropTableTemplate))

func NewDeleteObjectOperation() *DeleteObjectOperation {
	return &DeleteObjectOperation{}
}
