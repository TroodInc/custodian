package field

import (
	"custodian/server/transactions"
	"database/sql"
	"custodian/server/migrations/operations/field"
	"custodian/server/pg"
	"fmt"
	"custodian/logger"
	"custodian/server/pg/migrations/operations/statement_factories"
	"custodian/server/object/description"
	"custodian/server/object/meta"
)

type RemoveFieldOperation struct {
	field.RemoveFieldOperation
}

func (o *RemoveFieldOperation) SyncDbDescription(metaDescription *description.MetaDescription, transaction transactions.DbTransaction, syncer meta.MetaDescriptionSyncer) (err error) {
	tx := transaction.Transaction().(*sql.Tx)

	columns, ifk, _, seq, err := pg.NewMetaDdlFactory(syncer).FactoryFieldProperties(o.Field, metaDescription)
	if err != nil {
		return err
	}
	var statementSet = pg.DdlStatementSet{}

	if err := o.addSequenceStatement(&statementSet, seq); err != nil {
		return err
	}

	if err := o.addConstraintStatement(&statementSet, ifk, metaDescription); err != nil {
		return err
	}
	if err := o.addColumnStatements(&statementSet, columns, metaDescription); err != nil {
		return err
	}
	if err := o.addEnumStatements(&statementSet, columns, metaDescription); err != nil {
		return err
	}
	for _, statement := range statementSet {
		logger.Debug("Removing field from DB: %s\n", statement.Code)
		if _, err = tx.Exec(statement.Code); err != nil {
			return pg.NewDdlError(metaDescription.Name, pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
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

func (o *RemoveFieldOperation) addColumnStatements(statementSet *pg.DdlStatementSet, columns []pg.Column, metaDescription *description.MetaDescription) error {
	statementFactory := new(statement_factories.ColumnStatementFactory)
	tableName := pg.GetTableName(metaDescription.Name)
	for _, column := range columns {
		statement, err := statementFactory.FactoryDropStatement(tableName, column)
		if err != nil {
			return err
		}
		statementSet.Add(statement)
	}
	return nil
}

func (o *RemoveFieldOperation) addConstraintStatement(statementSet *pg.DdlStatementSet, ifk *pg.IFK, metaDescription *description.MetaDescription) error {
	if ifk == nil {
		return nil
	}
	statementFactory := new(statement_factories.ConstraintStatementFactory)
	tableName := pg.GetTableName(metaDescription.Name)

	statement, err := statementFactory.FactoryDropIFKStatement(tableName, ifk)
	if err != nil {
		return err
	}
	statementSet.Add(statement)
	return nil
}

func (o *RemoveFieldOperation) addEnumStatements(statementSet *pg.DdlStatementSet, columns []pg.Column, metaDescription *description.MetaDescription) error {
	tableName := pg.GetTableName(metaDescription.Name)
	for _, column := range columns {
		if len(column.Enum) > 0 {
			statement, err := pg.DropEnumStatement(tableName, column.Name)
			if err != nil {
				return err
			}
			statementSet.Add(statement)
		}
	}
	return nil
}

func NewRemoveFieldOperation(targetField *description.Field) *RemoveFieldOperation {
	return &RemoveFieldOperation{field.RemoveFieldOperation{Field: targetField}}
}
