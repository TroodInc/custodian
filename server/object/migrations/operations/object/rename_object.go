package object

import (
	"custodian/logger"
	"custodian/server/migrations/operations/object"
	object2 "custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/object/migrations/operations/statement_factories"
	"custodian/server/transactions"

	"fmt"
)

type RenameObjectOperation struct {
	object.RenameObjectOperation
}

func (o *RenameObjectOperation) SyncDbDescription(metaDescription *description.MetaDescription, transaction transactions.DbTransaction, syncer object2.MetaDescriptionSyncer) (err error) {
	tx := transaction.Transaction()

	//rename table
	var statementSet = object2.DdlStatementSet{}

	err = o.factoryTableStatements(&statementSet, metaDescription.Name, o.MetaDescription.Name)
	if err != nil {
		return err
	}

	for i := range metaDescription.Fields {
		currentField := metaDescription.Fields[i]
		newField := o.MetaDescription.FindField(currentField.Name)

		_, _, _, newSequence, err := object2.NewMetaDdlFactory(syncer).FactoryFieldProperties(newField, o.MetaDescription)
		if err != nil {
			return err
		}
		_, _, _, currentSequence, err := object2.NewMetaDdlFactory(syncer).FactoryFieldProperties(&currentField, metaDescription)
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
			return object2.NewDdlError(metaDescription.Name, object2.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
		}
	}

	return nil
}

func (o *RenameObjectOperation) factoryTableStatements(statementSet *object2.DdlStatementSet, currentName string, newName string) error {
	statementFactory := new(statement_factories.TableStatementFactory)
	statement, err := statementFactory.FactoryRenameStatement(object2.GetTableName(currentName), object2.GetTableName(newName))
	if err != nil {
		return err
	}
	statementSet.Add(statement)
	return nil
}

func (o *RenameObjectOperation) factorySequenceStatements(statementSet *object2.DdlStatementSet, currentSequence *object2.Seq, newSequence *object2.Seq) error {
	statementFactory := new(statement_factories.SequenceStatementFactory)
	statement, err := statementFactory.FactoryRenameStatement(currentSequence, newSequence)
	if err != nil {
		return err
	}
	statementSet.Add(statement)

	return nil
}

func NewRenameObjectOperation(metaDescription *description.MetaDescription) *RenameObjectOperation {
	return &RenameObjectOperation{RenameObjectOperation: object.RenameObjectOperation{MetaDescription: metaDescription}}
}
