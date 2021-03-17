package field

import (
	"custodian/logger"
	"custodian/server/migrations/operations/field"
	"custodian/server/object"
	meta_description "custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/object/migrations/operations/statement_factories"
	"custodian/server/transactions"
	"database/sql"
	"fmt"
)

type AddFieldOperation struct {
	field.AddFieldOperation
}

func (o *AddFieldOperation) SyncDbDescription(metaDescriptionToApply *meta_description.MetaDescription, transaction transactions.DbTransaction, syncer meta.MetaDescriptionSyncer) (err error) {
	tx := transaction.Transaction().(*sql.Tx)

	columns, ifk, _, seq, err := object.NewMetaDdlFactory(syncer).FactoryFieldProperties(o.Field, metaDescriptionToApply)
	if err != nil {
		return err
	}
	var statementSet = object.DdlStatementSet{}

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
			return object.NewDdlError(metaDescriptionToApply.Name, object.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
		}
	}

	return nil
}

func (o *AddFieldOperation) addSequenceStatement(statementSet *object.DdlStatementSet, sequence *object.Seq) error {
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

func (o *AddFieldOperation) addColumnStatements(statementSet *object.DdlStatementSet, columns []object.Column, metaDescriptionToApply *meta_description.MetaDescription) error {
	statementFactory := new(statement_factories.ColumnStatementFactory)
	tableName := object.GetTableName(metaDescriptionToApply.Name)
	for _, column := range columns {
		if len(column.Enum) > 0 {
			if err := o.addEnumStatement(statementSet, metaDescriptionToApply); err != nil {
				return err
			}

			statement, err := statementFactory.FactoryAddEnumStatement(tableName, column)
			if err != nil {
				return err
			}
			statementSet.Add(statement)

		} else {
			statement, err := statementFactory.FactoryAddStatement(tableName, column)
			if err != nil {
				return err
			}
			statementSet.Add(statement)
		}
	}
	return nil
}

func (o *AddFieldOperation) addConstraintStatement(statementSet *object.DdlStatementSet, ifk *object.IFK, metaDescriptionToApply *meta_description.MetaDescription) error {
	if ifk == nil {
		return nil
	}
	statementFactory := new(statement_factories.ConstraintStatementFactory)
	tableName := object.GetTableName(metaDescriptionToApply.Name)

	statement, err := statementFactory.FactoryCreateIFKStatement(tableName, ifk)
	if err != nil {
		return err
	}
	statementSet.Add(statement)
	return nil
}

func (o *AddFieldOperation) addEnumStatement(statementSet *object.DdlStatementSet, metaDescriptionToApply *meta_description.MetaDescription) error {
	tableName := object.GetTableName(metaDescriptionToApply.Name)
	statement, err := object.CreateEnumStatement(tableName, o.Field.Name, o.Field.Enum)
	if err != nil {
		return err
	}
	statementSet.Add(statement)

	return nil
}

func NewAddFieldOperation(targetField *meta_description.Field) *AddFieldOperation {
	return &AddFieldOperation{field.AddFieldOperation{Field: targetField}}
}
