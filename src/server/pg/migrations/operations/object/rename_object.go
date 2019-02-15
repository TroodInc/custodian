package object

import (
	"server/migrations/operations/object"
	"database/sql"
	"server/pg"
	"logger"
	"server/transactions"
	"fmt"
	"server/pg/migrations/operations/statement_factories"
	"server/object/description"
	"server/object/meta"
)

type RenameObjectOperation struct {
	object.RenameObjectOperation
}

func (o *RenameObjectOperation) SyncDbDescription(metaDescription *description.MetaDescription, transaction transactions.DbTransaction, syncer meta.MetaDescriptionSyncer) (err error) {
	tx := transaction.Transaction().(*sql.Tx)

	//rename table
	var statementSet = pg.DdlStatementSet{}

	err = o.factoryTableStatements(&statementSet, metaDescription.Name, o.MetaDescription.Name)
	if err != nil {
		return err
	}

	for i := range metaDescription.Fields {
		currentField := metaDescription.Fields[i]
		newField := o.MetaDescription.FindField(currentField.Name)

		_, _, _, newSequence, err := pg.NewMetaDdlFactory(syncer).FactoryFieldProperties(newField, o.MetaDescription)
		if err != nil {
			return err
		}
		_, _, _, currentSequence, err := pg.NewMetaDdlFactory(syncer).FactoryFieldProperties(&currentField, metaDescription)
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
			return pg.NewDdlError(metaDescription.Name, pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%statement': %statement", statement.Name, err.Error()))
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

func NewRenameObjectOperation(metaDescription *description.MetaDescription) *RenameObjectOperation {
	return &RenameObjectOperation{RenameObjectOperation: object.RenameObjectOperation{MetaDescription: metaDescription}}
}
