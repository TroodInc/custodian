package field

import (
	"server/object/meta"
	"server/transactions"
	"database/sql"
	"server/migrations/operations/field"
	"server/pg"
	"fmt"
	"logger"
	"server/pg/migrations/operations/statement_factories"
)

type RemoveFieldOperation struct {
	field.RemoveFieldOperation
}

func (o *RemoveFieldOperation) SyncDbDescription(metaObj *meta.Meta, transaction transactions.DbTransaction) (err error) {
	tx := transaction.Transaction().(*sql.Tx)

	columns, ifk, _, seq, err := new(pg.MetaDdlFactory).FactoryFieldProperties(o.Field)
	if err != nil {
		return err
	}
	var statementSet = pg.DdlStatementSet{}

	if err := o.addSequenceStatement(&statementSet, seq); err != nil {
		return err
	}

	if err := o.addConstraintStatement(&statementSet, ifk, metaObj); err != nil {
		return err
	}
	if err := o.addColumnStatements(&statementSet, columns, metaObj); err != nil {
		return err
	}

	for _, statement := range statementSet {
		logger.Debug("Removing field from DB: %s\n", statement.Code)
		if _, err = tx.Exec(statement.Code); err != nil {
			return pg.NewDdlError(metaObj.Name, pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
		}
	}

	return nil
}

func (o *RemoveFieldOperation) addSequenceStatement(statementSet *pg.DdlStatementSet, sequence *pg.Seq) error {
	if sequence == nil {
		return nil
	}
	if statement, err := new(statement_factories.SequenceStatementFactory).FactoryDropStatement(sequence); err != nil {
		return err
	} else {
		statementSet.Add(statement)
		return nil
	}
}

func (o *RemoveFieldOperation) addColumnStatements(statementSet *pg.DdlStatementSet, columns []pg.Column, metaObj *meta.Meta) error {
	statementFactory := new(statement_factories.ColumnStatementFactory)
	tableName := pg.GetTableName(metaObj.Name)
	for _, column := range columns {
		statement, err := statementFactory.FactoryDropStatement(tableName, column)
		if err != nil {
			return err
		}
		statementSet.Add(statement)
	}
	return nil
}

func (o *RemoveFieldOperation) addConstraintStatement(statementSet *pg.DdlStatementSet, ifk *pg.IFK, metaObj *meta.Meta) error {
	if ifk == nil {
		return nil
	}
	statementFactory := new(statement_factories.ConstraintStatementFactory)
	tableName := pg.GetTableName(metaObj.Name)

	statement, err := statementFactory.FactoryDropStatement(tableName, ifk)
	if err != nil {
		return err
	}
	statementSet.Add(statement)
	return nil
}

func NewRemoveFieldOperation(targetField *meta.FieldDescription) *RemoveFieldOperation {
	return &RemoveFieldOperation{field.RemoveFieldOperation{Field: targetField}}
}
