package transactions

import (
	"server/data"
	"server/transactions"
	"database/sql"
)

type PgDbTransactionManager struct {
	dataManager data.DataManager
}

//transaction related methods
func (tm *PgDbTransactionManager) BeginTransaction() (transactions.DbTransaction, error) {
	return tm.dataManager.Db().(*sql.DB).Begin()
}

func (tm *PgDbTransactionManager) CommitTransaction(dbTransaction transactions.DbTransaction) (error) {
	tx := dbTransaction.(*sql.Tx)
	if err := tx.Commit(); err != nil {
		return NewTransactionError(ErrCommitFailed, err.Error())
	}
	return nil
}

func (tm *PgDbTransactionManager) RollbackTransaction(dbTransaction transactions.DbTransaction) (error) {
	return dbTransaction.(*sql.Tx).Rollback()
}

func NewPgDbTransactionManager(dataManager data.DataManager) *PgDbTransactionManager {
	return &PgDbTransactionManager{dataManager: dataManager}
}
