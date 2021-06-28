package transactions

import "database/sql"

type DbTransaction interface {
	Execute([]Operation) error
	Complete() error
	Close() error
	Transaction() *sql.Tx
}
