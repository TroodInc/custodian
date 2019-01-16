package field

import (
	"server/transactions"
	"database/sql"
	"server/migrations/operations/field"
	"server/migrations"
	"server/pg"
	"fmt"
	"logger"
	"server/pg/migrations/operations/statement_factories"
	"server/object/description"
	"server/object/meta"
)

type UpdateFieldOperation struct {
	field.UpdateFieldOperation
}

func (o *UpdateFieldOperation) SyncDbDescription(metaDescription *description.MetaDescription, transaction transactions.DbTransaction, syncer meta.MetaDescriptionSyncer) (err error) {
	tx := transaction.Transaction().(*sql.Tx)

	newColumns, newIfk, _, newSequence, err := pg.NewMetaDdlFactory(syncer).FactoryFieldProperties(o.NewField, metaDescription)
	if err != nil {
		return err
	}
	currentColumns, currentIfk, _, currentSequence, err := pg.NewMetaDdlFactory(syncer).FactoryFieldProperties(o.CurrentField, metaDescription)
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
func (o *UpdateFieldOperation) factoryColumnsStatements(statementSet *pg.DdlStatementSet, currentColumns []pg.Column, newColumns []pg.Column, metaDescription *description.MetaDescription) error {
	statementFactory := new(statement_factories.ColumnStatementFactory)
	tableName := pg.GetTableName(metaDescription.Name)
	if len(currentColumns) != len(newColumns) {
		return migrations.NewMigrationError("Update column migration cannot be done with difference numbers of columns")
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
		}
	}
	return nil
}

func (o *UpdateFieldOperation) factoryConstraintStatement(statementSet *pg.DdlStatementSet, currentIfk *pg.IFK, newIfk *pg.IFK, metaDescription *description.MetaDescription) error {
	statementFactory := new(statement_factories.ConstraintStatementFactory)
	tableName := pg.GetTableName(metaDescription.Name)
	if currentIfk != nil {
		statement, err := statementFactory.FactoryDropStatement(tableName, currentIfk)
		if err != nil {
			return err
		}
		statementSet.Add(statement)
	}
	if newIfk != nil {
		statement, err := statementFactory.FactoryCreateStatement(tableName, newIfk)
		if err != nil {
			return err
		}
		statementSet.Add(statement)
	}
	return nil
}

func NewUpdateFieldOperation(currentField *description.Field, newField *description.Field) *UpdateFieldOperation {
	return &UpdateFieldOperation{field.UpdateFieldOperation{CurrentField: currentField, NewField: newField}}
}
