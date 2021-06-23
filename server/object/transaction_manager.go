package object

import (
	"custodian/server/transactions"
	"database/sql"
)

type PgDbTransactionManager struct {
	db *sql.DB
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

func NewPgDbTransactionManager(db *sql.DB) *PgDbTransactionManager {
	return &PgDbTransactionManager{db: db}
}
