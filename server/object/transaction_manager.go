package object

import (
	"custodian/server/transactions"
	"database/sql"
)

type PgDbTransactionManager struct {
	dataManager *DBManager
	transaction *PgTransaction
	doneStream  chan bool
}

//transaction related methods
func (tm *PgDbTransactionManager) BeginTransaction() (transactions.DbTransaction, error) {
	if tm.transaction == nil {
		go func() { tm.doneStream <- true }()
		if tx, err := tm.dataManager.Db().(*sql.DB).Begin(); err != nil {
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

func (tm *PgDbTransactionManager) CommitTransaction(dbTransaction transactions.DbTransaction) error {
	if tm.transaction.Counter == 0 {
		tx := dbTransaction.Transaction().(*sql.Tx)
		if err := tx.Commit(); err != nil {
			<-tm.doneStream
			return NewTransactionError(ErrCommitFailed, err.Error())
		}
		<-tm.doneStream
		tm.transaction = nil
	} else {
		tm.transaction.Counter -= 1
		//fmt.Println("Commit DB transaction [", tm.transaction.Counter, "]")
	}
	return nil
}

func (tm *PgDbTransactionManager) RollbackTransaction(dbTransaction transactions.DbTransaction) error {
	if tm.transaction.Counter == 0 {
		<-tm.doneStream
		tm.transaction = nil
		return dbTransaction.Transaction().(*sql.Tx).Rollback()
	} else {
		tm.transaction.Counter -= 1
		//fmt.Println("Rollback DB transaction [", tm.transaction.Counter, "]")
		return nil
	}
}

func NewPgDbTransactionManager(dataManager *DBManager) *PgDbTransactionManager {
	done := make(chan bool)
	return &PgDbTransactionManager{dataManager: dataManager, doneStream: done}
}
