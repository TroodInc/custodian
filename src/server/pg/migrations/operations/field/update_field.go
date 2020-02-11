package field

import (
	"database/sql"
	"fmt"
	"logger"
	"server/errors"
	"server/migrations"
	"server/migrations/operations/field"
	"server/object"
	"server/pg"
	"server/pg/migrations/operations/statement_factories"
	"server/transactions"
)

type UpdateFieldOperation struct {
	field.UpdateFieldOperation
}

func (o *UpdateFieldOperation) SyncDbDescription(metaDescription *object.Meta, transaction transactions.DbTransaction, syncer object.MetaDescriptionSyncer) (err error) {
	tx := transaction.Transaction().(*sql.Tx)

	newColumns, newIfk, _, newSequence, err := pg.NewMetaDdlFactory(syncer).FactoryFieldProperties(o.NewField, metaDescription.Name, metaDescription.Key)
	if err != nil {
		return err
	}
	currentColumns, currentIfk, _, currentSequence, err := pg.NewMetaDdlFactory(syncer).FactoryFieldProperties(o.CurrentField, metaDescription.Name, metaDescription.Key)
	if err != nil {
		return err
	}

	var statementSet = pg.DdlStatementSet{}
	//sequence
	if err := o.factorySequenceStatements(&statementSet, currentSequence, newSequence); err != nil {
		return err
	}
	//columns
	if err := o.factoryColumnsStatements(&statementSet, currentColumns, newColumns, metaDescription); err != nil {
		return err
	}
	//constraint
	if err := o.factoryConstraintStatement(&statementSet, currentIfk, newIfk, metaDescription); err != nil {
		return err
	}

	for _, statement := range statementSet {
		logger.Debug("Updating field in DB: %s\n", statement.Code)
		if _, err = tx.Exec(statement.Code); err != nil {
			return pg.NewDdlError(metaDescription.Name, pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
		}
	}

	return nil
}

func (o *UpdateFieldOperation) factorySequenceStatements(statementSet *pg.DdlStatementSet, currentSequence *pg.Seq, newSequence *pg.Seq) error {
	statementFactory := new(statement_factories.SequenceStatementFactory)
	if currentSequence == nil && newSequence != nil {
		//new sequence added case
		statement, err := statementFactory.FactoryCreateStatement(newSequence)
		if err != nil {
			return err
		}
		statementSet.Add(statement)
	} else if currentSequence != nil && newSequence == nil {
		//sequence is being dropped
		statement, err := statementFactory.FactoryDropStatement(currentSequence)
		if err != nil {
			return err
		}
		statementSet.Add(statement)
	} else {
		if currentSequence != nil && newSequence != nil {
			//	sequence is being renamed due to field renaming
			if currentSequence.Name != newSequence.Name {
				statement, err := statementFactory.FactoryRenameStatement(currentSequence, newSequence)
				if err != nil {
					return err
				}
				statementSet.Add(statement)
			}
		}
	}
	return nil
}

// column
func (o *UpdateFieldOperation) factoryColumnsStatements(statementSet *pg.DdlStatementSet, currentColumns []pg.Column, newColumns []pg.Column, metaDescription *object.Meta) error {
	statementFactory := new(statement_factories.ColumnStatementFactory)
	constraintFactory := new(statement_factories.ConstraintStatementFactory)
	tableName := pg.GetTableName(metaDescription.Name)
	if len(currentColumns) != len(newColumns) {
		return errors.NewFatalError(migrations.MigrationErrorInvalidDescription, "Update column migration cannot be done with difference numbers of columns", nil)
	} else {
		var currentColumn pg.Column
		var newColumn pg.Column
		for i := range currentColumns {
			currentColumn = currentColumns[i]
			newColumn = newColumns[i]
			if currentColumn.Name != newColumn.Name {
				//process renaming
				statement, err := statementFactory.FactoryRenameStatement(tableName, currentColumn, newColumn)
				if err != nil {
					return err
				} else {
					statementSet.Add(statement)
				}
			}
			if currentColumn.Optional != newColumn.Optional {
				//process nullability change
				statement, err := statementFactory.FactorySetNullStatement(tableName, newColumn)
				if err != nil {
					return err
				} else {
					statementSet.Add(statement)
				}
			}
			if currentColumn.Defval != newColumn.Defval {
				//process default value change
				statement, err := statementFactory.FactorySetDefaultStatement(tableName, newColumn)
				if err != nil {
					return err
				} else {
					statementSet.Add(statement)
				}
			}
			if currentColumn.Typ != newColumn.Typ {
				//process type change
				statement, err := statementFactory.FactorySetTypeStatement(tableName, newColumn)
				if err != nil {
					return err
				} else {
					statementSet.Add(statement)
				}
			}
			if currentColumn.Unique != newColumn.Unique {
				//process unique constraint
				statement, err := constraintFactory.FactorySetUniqueStatement(tableName, newColumn)
				if err != nil {
					return err
				} else {
					statementSet.Add(statement)
				}
			}
		}
	}
	return nil
}

func (o *UpdateFieldOperation) factoryConstraintStatement(statementSet *pg.DdlStatementSet, currentIfk *pg.IFK, newIfk *pg.IFK, metaDescription *object.Meta) error {
	statementFactory := new(statement_factories.ConstraintStatementFactory)
	tableName := pg.GetTableName(metaDescription.Name)
	if currentIfk != nil {
		statement, err := statementFactory.FactoryDropIFKStatement(tableName, currentIfk)
		if err != nil {
			return err
		}
		statementSet.Add(statement)
	}
	if newIfk != nil {
		statement, err := statementFactory.FactoryCreateIFKStatement(tableName, newIfk)
		if err != nil {
			return err
		}
		statementSet.Add(statement)
	}
	return nil
}

func NewUpdateFieldOperation(currentField *object.Field, newField *object.Field) *UpdateFieldOperation {
	return &UpdateFieldOperation{field.UpdateFieldOperation{CurrentField: currentField, NewField: newField}}
}
