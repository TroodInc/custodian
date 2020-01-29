package field

import (
	meta_description "server/object/description"
	"server/transactions"
	"database/sql"
	"server/migrations/operations/field"
	"server/pg"
	"fmt"
	"logger"
	"server/pg/migrations/operations/statement_factories"
	"server/object/meta"
)

type AddFieldOperation struct {
	field.AddFieldOperation
}

func (o *AddFieldOperation) SyncDbDescription(metaDescriptionToApply *meta_description.MetaDescription, transaction transactions.DbTransaction, syncer meta.MetaDescriptionSyncer) (err error) {
	tx := transaction.Transaction().(*sql.Tx)

	columns, ifk, _, seq, err := pg.NewMetaDdlFactory(syncer).FactoryFieldProperties(o.Field, metaDescriptionToApply.Name, metaDescriptionToApply.Key)
	if err != nil {
		return err
	}
	var statementSet = pg.DdlStatementSet{}

	if err := o.addSequenceStatement(&statementSet, seq); err != nil {
		return err
	}

	if err := o.addColumnStatements(&statementSet, columns, metaDescriptionToApply); err != nil {
		return err
	}

	if err := o.addConstraintStatement(&statementSet, ifk, metaDescriptionToApply); err != nil {
		return err
	}

	for _, statement := range statementSet {
		logger.Debug("Creating field in DB: %s\n", statement.Code)
		if _, err = tx.Exec(statement.Code); err != nil {
			return pg.NewDdlError(metaDescriptionToApply.Name, pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
		}
	}

	return nil
}

func (o *AddFieldOperation) addSequenceStatement(statementSet *pg.DdlStatementSet, sequence *pg.Seq) error {
	if sequence == nil {
		return nil
	}
	if statement, err := new(statement_factories.SequenceStatementFactory).FactoryCreateStatement(sequence); err != nil {
		return err
	} else {
		statementSet.Add(statement)
		return nil
	}
}

func (o *AddFieldOperation) addColumnStatements(statementSet *pg.DdlStatementSet, columns []pg.Column, metaDescriptionToApply *meta_description.MetaDescription) error {
	statementFactory := new(statement_factories.ColumnStatementFactory)
	tableName := pg.GetTableName(metaDescriptionToApply.Name)
	for _, column := range columns {
		statement, err := statementFactory.FactoryAddStatement(tableName, column)
		if err != nil {
			return err
		}
		statementSet.Add(statement)
	}
	return nil
}

func (o *AddFieldOperation) addConstraintStatement(statementSet *pg.DdlStatementSet, ifk *pg.IFK, metaDescriptionToApply *meta_description.MetaDescription) error {
	if ifk == nil {
		return nil
	}
	statementFactory := new(statement_factories.ConstraintStatementFactory)
	tableName := pg.GetTableName(metaDescriptionToApply.Name)

	statement, err := statementFactory.FactoryCreateIFKStatement(tableName, ifk)
	if err != nil {
		return err
	}
	statementSet.Add(statement)
	return nil
}

func NewAddFieldOperation(targetField *meta_description.Field) *AddFieldOperation {
	return &AddFieldOperation{field.AddFieldOperation{Field: targetField}}
}
