package transactions

import "database/sql"

type DbTransaction interface {
	Execute([]Operation) error
	Commit() error
	Rollback() error
	Transaction() *sql.Tx
}
