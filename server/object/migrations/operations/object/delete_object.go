package object

import (
	"custodian/logger"
	"custodian/server/migrations/operations/object"
	object2 "custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/transactions"
	"database/sql"
	"fmt"
	"text/template"
)

type DeleteObjectOperation struct {
	object.DeleteObjectOperation
}

func (o *DeleteObjectOperation) SyncDbDescription(metaDescription *description.MetaDescription, transaction transactions.DbTransaction, syncer meta.MetaDescriptionSyncer) (err error) {
	tx := transaction.Transaction().(*sql.Tx)
	var metaDdl *object2.MetaDDL
	if metaDdl, err = object2.NewMetaDdlFactory(syncer).Factory(metaDescription); err != nil {
		return err
	}

	var statementSet = object2.DdlStatementSet{}

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
	
	for _, column := range metaDdl.Columns {
		if len(column.Enum) > 0 {
			statement, err := object2.DropEnumStatement(metaDdl.Table, column.Name)
			if err != nil {
				return err
			}

			statementSet.Add(statement)
		}
	}

	for _, statement := range statementSet {
		logger.Debug("Removing object in DB: %syncer\n", statement.Code)
		if _, err = tx.Exec(statement.Code); err != nil {
			return object2.NewDdlError(metaDescription.Name, object2.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
		}
	}
	return nil
}

const dropTableTemplate = `DROP TABLE "{{.Table}}" {{.Mode}};`

var parsedDropTableTemplate = template.Must(template.New("drop_table").Funcs(ddlFuncs).Parse(dropTableTemplate))

func NewDeleteObjectOperation() *DeleteObjectOperation {
	return &DeleteObjectOperation{}
}
