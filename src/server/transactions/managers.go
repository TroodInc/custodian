package transactions

import "server/object/description"

type DbTransactionManager interface {
	BeginTransaction() (DbTransaction, error)
	CommitTransaction(DbTransaction) (error)
	RollbackTransaction(DbTransaction) (error)
}

type MetaDescriptionTransactionManager interface {
	BeginTransaction(metaList []*description.MetaDescription) (MetaDescriptionTransaction, error)
	CommitTransaction(MetaDescriptionTransaction) (error)
	RollbackTransaction(MetaDescriptionTransaction) (error)
}

type GlobalTransactionManager struct {
	metaDescriptionTransactionManager MetaDescriptionTransactionManager
	dbTransactionManager              DbTransactionManager
}

func (g *GlobalTransactionManager) BeginTransaction(metaDescriptionList []*description.MetaDescription) (*GlobalTransaction, error) {
	dbTransaction, err := g.dbTransactionManager.BeginTransaction()
	if err != nil {
		return nil, err
	}
	metaDescriptionTransaction, err := g.metaDescriptionTransactionManager.BeginTransaction(metaDescriptionList)
	if err != nil {
		return nil, err
	}
	return &GlobalTransaction{DbTransaction: dbTransaction, MetaDescriptionTransaction: metaDescriptionTransaction}, nil
}

func (g *GlobalTransactionManager) CommitTransaction(transaction *GlobalTransaction) (error) {
	if err := g.dbTransactionManager.CommitTransaction(transaction.DbTransaction); err != nil {
		return err
	}
	if err := g.metaDescriptionTransactionManager.CommitTransaction(transaction.MetaDescriptionTransaction); err != nil {
		return err
	}
	return nil
}

func (g *GlobalTransactionManager) RollbackTransaction(transaction *GlobalTransaction) {
	g.dbTransactionManager.RollbackTransaction(transaction.DbTransaction)
	g.metaDescriptionTransactionManager.RollbackTransaction(transaction.MetaDescriptionTransaction)
}

func NewGlobalTransactionManager(metaDescriptionTransactionManager MetaDescriptionTransactionManager,
	dbTransactionManager DbTransactionManager) *GlobalTransactionManager {
	return &GlobalTransactionManager{
		metaDescriptionTransactionManager: metaDescriptionTransactionManager,
		dbTransactionManager:              dbTransactionManager,
	}
}
