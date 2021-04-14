package field

import (
	"custodian/logger"
	"custodian/server/errors"
	"custodian/server/migrations"
	"custodian/server/migrations/operations/field"
	"custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/migrations/operations/statement_factories"
	"custodian/server/transactions"

	"database/sql"
	"fmt"
)

type UpdateFieldOperation struct {
	field.UpdateFieldOperation
}

func (o *UpdateFieldOperation) SyncDbDescription(metaDescription *description.MetaDescription, transaction transactions.DbTransaction, syncer object.MetaDescriptionSyncer) (err error) {
	tx := transaction.Transaction().(*sql.Tx)

	newColumns, newIfk, _, newSequence, err := object.NewMetaDdlFactory(syncer).FactoryFieldProperties(o.NewField, metaDescription)
	if err != nil {
		return err
	}
	currentColumns, currentIfk, _, currentSequence, err := object.NewMetaDdlFactory(syncer).FactoryFieldProperties(o.CurrentField, metaDescription)
	if err != nil {
		return err
	}

	var statementSet = object.DdlStatementSet{}
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
			return errors.NewValidationError(
				object.ErrExecutingDDL,
				fmt.Sprintf("Can't update field: %s", err.Error()),
				map[string]string{"fields": o.CurrentField.Name},
			)
		}
	}

	return nil
}

func (o *UpdateFieldOperation) factorySequenceStatements(statementSet *object.DdlStatementSet, currentSequence *object.Seq, newSequence *object.Seq) error {
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
func (o *UpdateFieldOperation) factoryColumnsStatements(statementSet *object.DdlStatementSet, currentColumns []object.Column, newColumns []object.Column, metaDescription *description.MetaDescription) error {
	statementFactory := new(statement_factories.ColumnStatementFactory)
	constraintFactory := new(statement_factories.ConstraintStatementFactory)
	tableName := object.GetTableName(metaDescription.Name)
	if len(currentColumns) != len(newColumns) {
		return errors.NewFatalError(migrations.MigrationErrorInvalidDescription, "Update column migration cannot be done with difference numbers of columns", nil)
	} else {
		var currentColumn object.Column
		var newColumn object.Column
		for i := range currentColumns {
			currentColumn = currentColumns[i]
			newColumn = newColumns[i]
			if currentColumn.Name != newColumn.Name {
				//process renaming
				statement, err := statementFactory.FactoryRenameStatement(tableName, currentColumn, newColumn)
				if err != nil {
					return err
				}
				statementSet.Add(statement)
				if currentColumn.Typ == description.FieldTypeEnum {
					statement, err = object.RenameEnumStatement(tableName, currentColumn.Name, newColumn.Name)
					if err != nil {
						return err
					}
					statementSet.Add(statement)
				}
			}
			if currentColumn.Optional != newColumn.Optional {
				//process nullability change
				statement, err := statementFactory.FactorySetNullStatement(tableName, newColumn)
				if err != nil {
					return err
				}
				statementSet.Add(statement)
			}
			if currentColumn.Typ != newColumn.Typ {
				if len(newColumn.Enum) > 0 {
					statement, err := object.CreateEnumStatement(tableName, newColumn.Name, newColumn.Enum)
					if err != nil {
						return err
					}
					statementSet.Add(statement)
				}
				//process type change
				statement, err := statementFactory.FactorySetTypeStatement(tableName, newColumn)
				if err != nil {
					return err
				}
				statementSet.Add(statement)
				if len(currentColumn.Enum) > 0 {
					statement, err := statementFactory.FactoryDropDefaultStatement(tableName, newColumn)
					if err != nil {
						return err
					}
					statementSet.Add(statement)
					statement, err = object.DropEnumStatement(tableName, newColumn.Name)
					if err != nil {
						return err
					}
					statementSet.Add(statement)
				}
			}
			if newColumn.Typ == description.FieldTypeEnum && len(newColumn.Enum) > 0 {
				if !object.ChoicesIsCompleting(currentColumn.Enum, newColumn.Enum) {
					return errors.NewValidationError("400", fmt.Sprintf("Table %s. Not all values are entered for the column `%s`. Necessary minimum: %v", tableName, newColumn.Name, currentColumn.Enum), nil)
				}
				for _ , choice := range newColumn.Enum {
					statement, err := object.AddEnumStatement(tableName, newColumn.Name, choice)
					if err != nil {
						return err
					}
					statementSet.Add(statement)
				}
			}

			if currentColumn.Defval != newColumn.Defval {
				//process default value change
				statement, err := statementFactory.FactorySetDefaultStatement(tableName, newColumn)
				if err != nil {
					return err
				}
				statementSet.Add(statement)
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

func (o *UpdateFieldOperation) factoryConstraintStatement(statementSet *object.DdlStatementSet, currentIfk *object.IFK, newIfk *object.IFK, metaDescription *description.MetaDescription) error {
	statementFactory := new(statement_factories.ConstraintStatementFactory)
	tableName := object.GetTableName(metaDescription.Name)
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

func NewUpdateFieldOperation(currentField *description.Field, newField *description.Field) *UpdateFieldOperation {
	return &UpdateFieldOperation{field.UpdateFieldOperation{CurrentField: currentField, NewField: newField}}
}
