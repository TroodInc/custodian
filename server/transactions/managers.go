package transactions

type DbTransactionManager interface {
	BeginTransaction() (DbTransaction, error)
	CommitTransaction(DbTransaction) error
	RollbackTransaction(DbTransaction) error
}