package transactions

type DbTransactionManager interface {
	BeginTransaction() (DbTransaction, error)
}