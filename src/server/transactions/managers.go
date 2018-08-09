package transactions

import "server/meta"

type DbTransactionManager interface{}

type MetaDescriptionTransactionManager interface {
	BeginTransaction(metaList []*object.MetaDescription) (MetaDescriptionTransaction, error)
	CommitTransaction(transaction MetaDescriptionTransaction) (error)
}

type GlobalTransactionManager struct {
	metaDescriptionTransactionManager MetaDescriptionTransactionManager
	dbTransactionManager              DbTransactionManager
}

func (g *GlobalTransactionManager) BeginTransaction(metaDescriptionList []*object.MetaDescription) (*GlobalTransaction, error) {
	dbTransaction, err := g.dbTransactionManager.BeginTransaction()
	if err != nil {
		return nil, err
	}
	metaDescriptionTransaction, err := g.metaDescriptionTransactionManager.BeginTransaction(metaDescriptionList)
	if err != nil {
		return nil, err
	}
	return &GlobalTransaction{DbTransaction: &dbTransaction, MetaDescriptionTransaction: &metaDescriptionTransaction}, nil
}

func (g *GlobalTransactionManager) CommitTransaction(transaction *GlobalTransaction) (error) {
	if err := metaStore.syncer.CommitTransaction(transaction.DbTransaction); err != nil {
		return err
	}
	if err := metaStore.drv.CommitTransaction(transaction.MetaDescriptionTransaction); err != nil {
		return err
	}
	return nil
}

func (g *GlobalTransactionManager) RollbackTransaction(transaction *GlobalTransaction) {
	metaStore.syncer.RollbackTransaction(transaction.DbTransaction)
	metaStore.drv.RollbackTransaction(transaction.MetaDescriptionTransaction)
}

func NewGlobalTransactionManager(metaDescriptionTransactionManager MetaDescriptionTransactionManager,
	dbTransactionManager DbTransactionManager) *GlobalTransactionManager {
	return &GlobalTransactionManager{
		metaDescriptionTransactionManager: metaDescriptionTransactionManager,
		dbTransactionManager:              dbTransactionManager,
	}
}
