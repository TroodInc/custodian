package object

import (
	"custodian/server/transactions"
	"database/sql"
	// "fmt"
)

type PgDbTransactionManager struct {
	dataManager *DBManager
	transaction *PgTransaction
}

//transaction related methods
func (tm *PgDbTransactionManager) BeginTransaction() (transactions.DbTransaction, error) {
	tx, err := tm.dataManager.Db().(*sql.DB).Begin()
	if err != nil {
		return nil, err
	}
	// fmt.Println("NewTransaction begin")

	return &PgTransaction{tx}, err

}

func (tm *PgDbTransactionManager) CommitTransaction(dbTransaction transactions.DbTransaction) error {

	tx := dbTransaction.Transaction().(*sql.Tx)
	if err := tx.Commit(); err != nil {
		return NewTransactionError(ErrCommitFailed, err.Error())
	}

	return nil
}

func (tm *PgDbTransactionManager) RollbackTransaction(dbTransaction transactions.DbTransaction) error {

	return dbTransaction.Transaction().(*sql.Tx).Rollback()

}

func NewPgDbTransactionManager(dataManager *DBManager) *PgDbTransactionManager {
	return &PgDbTransactionManager{dataManager: dataManager}
}
