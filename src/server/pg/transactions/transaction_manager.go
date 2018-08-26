package transactions

import (
	"server/data"
	"server/transactions"
	"database/sql"
	"server/pg"
)

type PgDbTransactionManager struct {
	dataManager data.DataManager
}

//transaction related methods
func (tm *PgDbTransactionManager) BeginTransaction() (transactions.DbTransaction, error) {
	if tx, err := tm.dataManager.Db().(*sql.DB).Begin(); err != nil {
		return nil, err
	} else {
		return &pg.PgTransaction{Tx: tx}, nil
	}
}

func (tm *PgDbTransactionManager) CommitTransaction(dbTransaction transactions.DbTransaction) (error) {
	tx := dbTransaction.Transaction().(*sql.Tx)
	if err := tx.Commit(); err != nil {
		return NewTransactionError(ErrCommitFailed, err.Error())
	}
	return nil
}

func (tm *PgDbTransactionManager) RollbackTransaction(dbTransaction transactions.DbTransaction) (error) {
	return dbTransaction.Transaction().(*sql.Tx).Rollback()
}

func NewPgDbTransactionManager(dataManager data.DataManager) *PgDbTransactionManager {
	return &PgDbTransactionManager{dataManager: dataManager}
}
