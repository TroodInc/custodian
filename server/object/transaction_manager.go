package object

import (
	"custodian/server/transactions"
	"database/sql"
)

type PgDbTransactionManager struct {
	db          *sql.DB
	transaction *PgTransaction
}

//transaction related methods
func (tm *PgDbTransactionManager) BeginTransaction() (transactions.DbTransaction, error) {
	if tx, err := tm.db.Begin(); err != nil {
		return nil, err
	} else {
		return &PgTransaction{tx, tm, 0}, nil
	}
}

func (tm *PgDbTransactionManager) ExecStmt(statement string) error {
	globalTransaction, err := tm.BeginTransaction()
	if err != nil {
		return err
	}
	tx := globalTransaction.Transaction()
	_, err = tx.Exec(statement)
	if err != nil {
		globalTransaction.Rollback()
		return err
	}
	globalTransaction.Commit()
	return nil
}
func NewPgDbTransactionManager(db *sql.DB) *PgDbTransactionManager {
	return &PgDbTransactionManager{db: db}
}
