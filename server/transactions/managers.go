package transactions

import "custodian/server/object/description"

type DbTransactionManager interface {
	BeginTransaction() (DbTransaction, error)
	CommitTransaction(DbTransaction) error
	RollbackTransaction(DbTransaction) error
}

type MetaDescriptionTransactionManager interface {
	BeginTransaction(metaList []*description.MetaDescription) (MetaDescriptionTransaction, error)
	CommitTransaction(MetaDescriptionTransaction) error
	RollbackTransaction(MetaDescriptionTransaction) error
}