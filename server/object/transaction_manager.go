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
	if tm.transaction == nil {
		if tx, err := tm.db.Begin(); err != nil {
			return nil, err
		} else {
			tm.transaction = &PgTransaction{tx, tm, 0}
		}
	} else {
		tm.transaction.Counter += 1
		//fmt.Println("Get DB transaction [", tm.transaction.Counter, "]")
	}

	return tm.transaction, nil
}

func (tm *PgDbTransactionManager) CommitTransaction(dbTransaction transactions.DbTransaction) (error) {
	if tm.transaction.Counter == 0 {
		tx := dbTransaction.Transaction()
		if err := tx.Commit(); err != nil {
			return NewTransactionError(ErrCommitFailed, err.Error())
		}

		tm.transaction = nil
	} else {
		tm.transaction.Counter -= 1
		//fmt.Println("Commit DB transaction [", tm.transaction.Counter, "]")
	}
	return nil
}

func (tm *PgDbTransactionManager) RollbackTransaction(dbTransaction transactions.DbTransaction) (error) {
	if tm.transaction.Counter == 0 {
		tm.transaction = nil
		return dbTransaction.Transaction().Rollback()
	} else {
		tm.transaction.Counter -= 1
		//fmt.Println("Rollback DB transaction [", tm.transaction.Counter, "]")
		return nil
	}
}

func NewPgDbTransactionManager(db *sql.DB) *PgDbTransactionManager {
	return &PgDbTransactionManager{db: db}
}
