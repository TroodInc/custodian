package transactions

import (
	"database/sql"
	"custodian/server/data"
	"custodian/server/pg"
	"custodian/server/transactions"
)

type PgDbTransactionManager struct {
	dataManager data.DataManager
	transaction *pg.PgTransaction
}

//transaction related methods
func (tm *PgDbTransactionManager) BeginTransaction() (transactions.DbTransaction, error) {
	if tm.transaction == nil {
		if tx, err := tm.dataManager.Db().(*sql.DB).Begin(); err != nil {
			return nil, err
		} else {
			tm.transaction = &pg.PgTransaction{tx, tm, 0}
		}
	} else {
		tm.transaction.Counter += 1
		//fmt.Println("Get DB transaction [", tm.transaction.Counter, "]")
	}

	return tm.transaction, nil
}

func (tm *PgDbTransactionManager) CommitTransaction(dbTransaction transactions.DbTransaction) (error) {
	if tm.transaction.Counter == 0 {
		tx := dbTransaction.Transaction().(*sql.Tx)
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
		return dbTransaction.Transaction().(*sql.Tx).Rollback()
	} else {
		tm.transaction.Counter -= 1
		//fmt.Println("Rollback DB transaction [", tm.transaction.Counter, "]")
		return nil
	}
}

func NewPgDbTransactionManager(dataManager data.DataManager) *PgDbTransactionManager {
	return &PgDbTransactionManager{dataManager: dataManager}
}
