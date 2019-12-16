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
	MetaDescriptionTransactionManager MetaDescriptionTransactionManager
	DbTransactionManager              DbTransactionManager
	transaction *GlobalTransaction
}

func (g *GlobalTransactionManager) BeginTransaction(metaDescriptionList []*description.MetaDescription) (*GlobalTransaction, error) {
	if g.transaction == nil {
		dbTransaction, err := g.DbTransactionManager.BeginTransaction()
		if err != nil {
			return nil, err
		}
		metaDescriptionTransaction, err := g.MetaDescriptionTransactionManager.BeginTransaction(metaDescriptionList)
		if err != nil {
			return nil, err
		}
		g.transaction = &GlobalTransaction{metaDescriptionTransaction, dbTransaction,0}
	} else {
		g.transaction.Counter += 1
	}
	return g.transaction, nil
}

func (g *GlobalTransactionManager) CommitTransaction(transaction *GlobalTransaction) (error) {
	if g.transaction.Counter == 0 {
		if err := g.DbTransactionManager.CommitTransaction(transaction.DbTransaction); err != nil {
			return err
		}
		if err := g.MetaDescriptionTransactionManager.CommitTransaction(transaction.MetaDescriptionTransaction); err != nil {
			return err
		}

		g.transaction = nil
	} else {
		g.transaction.Counter -= 1
	}
	return nil
}

func (g *GlobalTransactionManager) RollbackTransaction(transaction *GlobalTransaction) {
	if g.transaction.Counter == 0 {
		g.DbTransactionManager.RollbackTransaction(transaction.DbTransaction)
		g.MetaDescriptionTransactionManager.RollbackTransaction(transaction.MetaDescriptionTransaction)

		g.transaction = nil
	} else {
		g.transaction.Counter -= 1
	}
}

func NewGlobalTransactionManager(metaDescriptionTransactionManager MetaDescriptionTransactionManager,
	dbTransactionManager DbTransactionManager) *GlobalTransactionManager {
	return &GlobalTransactionManager{
		MetaDescriptionTransactionManager: metaDescriptionTransactionManager,
		DbTransactionManager:              dbTransactionManager,
	}
}
