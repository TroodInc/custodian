package object

import (
	"database/sql"
	"errors"
	"fmt"
	"logger"
	"server/migrations/operations/object"
	"server/object/description"
	"server/object/meta"
	"server/pg"
	"server/transactions"
	"text/template"
)

type CreateObjectOperation struct {
	object.CreateObjectOperation
}

func (o *CreateObjectOperation) SyncDbDescription(_ *description.MetaDescription, transaction transactions.DbTransaction, syncer meta.MetaDescriptionSyncer) (err error) {
	tx := transaction.Transaction().(*sql.Tx)
	var metaDdl *pg.MetaDDL
	var statementSet = pg.DdlStatementSet{}

	if metaDdl, err = pg.NewMetaDdlFactory(syncer).Factory(o.MetaDescription); err != nil {
		return err
	}

	for _, c := range metaDdl.Columns {
		if len(c.Enum) > 0 {
			if enumStatement, err := c.GetEnumStatement(metaDdl.Table); err != nil {
				return err
			} else {
				statementSet.Add(enumStatement)
			}
		}
	}

	for i, _ := range metaDdl.Seqs {
		if statement, err := metaDdl.Seqs[i].CreateDdlStatement(); err != nil {
			return err
		} else {
			statementSet.Add(statement)
		}
	}
	if statement, err := metaDdl.CreateTableDdlStatement(); err != nil {
		return err
	} else {
		statementSet.Add(statement)
	}

	for _, statement := range statementSet {
		logger.Debug("Creating object in DB: %syncer\n", statement.Code)
		if _, err = tx.Exec(statement.Code); err != nil {
			return pg.NewDdlError(o.MetaDescription.Name, pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
		}
	}
	return nil
}

//Auxilary template functions
var ddlFuncs = template.FuncMap{"dict": dictionary}

//DDL create table templates
func dictionary(values ...interface{}) (map[string]interface{}, error) {
	if len(values)&1 != 0 {
		return nil, errors.New("count of arguments must be even")
	}
	dict := make(map[string]interface{}, len(values)>>1)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			return nil, errors.New("dictionary key must be of string type")
		}
		dict[key] = values[i+1]
	}
	return dict, nil
}

func NewCreateObjectOperation(metaDescription *description.MetaDescription) *CreateObjectOperation {
	return &CreateObjectOperation{object.CreateObjectOperation{MetaDescription: metaDescription}}
}
