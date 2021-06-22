package object

import (
	"custodian/server/transactions"
	"database/sql"
)

type PgTransaction struct {
	*sql.Tx
}

func (pt *PgTransaction) Prepare(q string) (*Stmt, error) {
	return NewStmt(pt.Tx, q)
}

func (pt *PgTransaction) Transaction() interface{} {
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

func (pt *PgTransaction) Complete() error {
	return pt.Tx.Commit()
}

func (pt *PgTransaction) Close() error {
	return pt.Tx.Rollback()
}
