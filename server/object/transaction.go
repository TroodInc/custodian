package object

import (
	"custodian/server/transactions"
	"database/sql"
)

type PgTransaction struct {
	*sql.Tx
	Manager transactions.DbTransactionManager
	Counter int
}

func (pt *PgTransaction) Prepare(q string) (*Stmt, error) {
	return NewStmt(pt.Tx, q)
}

func (pt *PgTransaction) Transaction() *sql.Tx {
	return pt.Tx
}

func (pt *PgTransaction) Execute(ops []transactions.Operation) error {
	for _, op := range ops {
		if err := op(pt); err != nil {
			return err
		}
	}
	return nil
}

func (pt *PgTransaction) Commit() error {
	if pt.Counter == 0 {
		return pt.Tx.Commit()
	} else {
		pt.Counter -= 1
	}
	return nil
}

func (pt *PgTransaction) Rollback() error {
	if pt.Counter == 0 {
		return pt.Tx.Rollback()
	} else {
		pt.Counter -= 1
	}
	return nil
}
