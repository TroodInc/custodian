package file_transaction

import (
	. "server/transactions"
	"server/meta/description"
)

type FileMetaDescriptionTransactionManager struct {
	RemoveMetaCallback func(name string) (bool, error)
	CreateMetaCallBack func(MetaDescriptionTransaction, description.MetaDescription) error
}

func (fm *FileMetaDescriptionTransactionManager) BeginTransaction(metaList []*description.MetaDescription) (MetaDescriptionTransaction, error) {
	// store initial state
	return NewFileMetaDescriptionTransaction(Pending, metaList), nil
}

func (fm *FileMetaDescriptionTransactionManager) CommitTransaction(transaction MetaDescriptionTransaction) (error) {
	if transaction.State() == Pending {
		transaction.State() = Committed
		return nil
	} else {
		return &TransactionError{"Meta driver is not in pending state"}
	}

}

func (fm *FileMetaDescriptionTransactionManager) RollbackTransaction(transaction *FileMetaDescriptionTransaction) (error) {
	if transaction.State() != Pending {
		return &TransactionError{"Meta driver is not in pending state"}
	}
	//remove created meta
	for _, metaName := range transaction.CreatedMetaNameList() {
		fm.RemoveMetaCallback(metaName)
	}
	//restore initial state
	for _, metaDescription := range transaction.InitialMetaList() {
		fm.CreateMetaCallBack(transaction, *metaDescription)
	}
	transaction.SetState(RolledBack)
	return nil
}
