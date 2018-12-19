package object

import (
	"server/migrations/operations/object"
	"server/object/meta"
	"database/sql"
	"server/pg"
	"logger"
	"server/transactions"
	"fmt"
	"server/pg/migrations/operations/statement_factories"
)

type RenameObjectOperation struct {
	object.RenameObjectOperation
}

func (o *RenameObjectOperation) SyncDbDescription(metaObjToApply *meta.Meta, transaction transactions.DbTransaction) (err error) {
	tx := transaction.Transaction().(*sql.Tx)

	//rename table
	var statementSet = pg.DdlStatementSet{}

	err = o.factoryTableStatements(&statementSet, metaObjToApply.Name, o.Meta.Name)
	if err != nil {
		return err
	}

	for i := range metaObjToApply.Fields {
		currentField := metaObjToApply.Fields[i]
		newField := o.Meta.FindField(currentField.Name)

		_, _, _, newSequence, err := new(pg.MetaDdlFactory).FactoryFieldProperties(newField)
		if err != nil {
			return err
		}
		_, _, _, currentSequence, err := new(pg.MetaDdlFactory).FactoryFieldProperties(&currentField)
		if err != nil {
			return err
		}
		if newSequence != nil && currentSequence != nil && newSequence.Name != currentSequence.Name {
			o.factorySequenceStatements(&statementSet, currentSequence, newSequence)
		}
	}

	for _, statement := range statementSet {
		logger.Debug("Renaming object: %s\n", statement.Code)
		if _, err = tx.Exec(statement.Code); err != nil {
			return pg.NewDdlError(metaObjToApply.Name, pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
		}
	}

	return nil
}

func (o *RenameObjectOperation) factoryTableStatements(statementSet *pg.DdlStatementSet, currentName string, newName string) error {
	statementFactory := new(statement_factories.TableStatementFactory)
	statement, err := statementFactory.FactoryRenameStatement(pg.GetTableName(currentName), pg.GetTableName(newName))
	if err != nil {
		return err
	}
	statementSet.Add(statement)
	return nil
}

func (o *RenameObjectOperation) factorySequenceStatements(statementSet *pg.DdlStatementSet, currentSequence *pg.Seq, newSequence *pg.Seq) error {
	statementFactory := new(statement_factories.SequenceStatementFactory)
	statement, err := statementFactory.FactoryRenameStatement(currentSequence, newSequence)
	if err != nil {
		return err
	}
	statementSet.Add(statement)

	return nil
}

func NewRenameObjectOperation(metaObj *meta.Meta) *RenameObjectOperation {
	return &RenameObjectOperation{RenameObjectOperation: object.RenameObjectOperation{Meta: metaObj}}
}
