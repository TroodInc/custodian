package file_transaction

// TODO drop this package
import (
	"custodian/server/object/description"
	. "custodian/server/transactions"
)

type FileMetaDescriptionTransactionManager struct {
	RemoveMetaCallback func(name string) (bool, error)
	CreateMetaCallBack func(description.MetaDescription) error
}

func (fm *FileMetaDescriptionTransactionManager) BeginTransaction(metaList []*description.MetaDescription) (MetaDescriptionTransaction, error) {
	// store initial state
	return NewFileMetaDescriptionTransaction(Pending, metaList), nil
}

func (fm *FileMetaDescriptionTransactionManager) CommitTransaction(transaction MetaDescriptionTransaction) error {
	if transaction.State() == Pending {
		transaction.SetState(Committed)
		return nil
	} else {
		return &TransactionError{"MetaDescription driver is not in pending state"}
	}

}

func (fm *FileMetaDescriptionTransactionManager) RollbackTransaction(transaction MetaDescriptionTransaction) error {
	if transaction.State() != Pending {
		return &TransactionError{"MetaDescription driver is not in pending state"}
	}
	//remove created meta
	for _, metaName := range transaction.CreatedMetaNameList() {
		fm.RemoveMetaCallback(metaName)
	}
	//restore initial state
	for _, metaDescription := range transaction.InitialMetaList() {
		fm.CreateMetaCallBack(*metaDescription)
	}
	transaction.SetState(RolledBack)
	return nil
}

func NewFileMetaDescriptionTransactionManager(RemoveMetaCallback func(name string) (bool, error),
	CreateMetaCallBack func(description.MetaDescription) error) *FileMetaDescriptionTransactionManager {
	return &FileMetaDescriptionTransactionManager{RemoveMetaCallback: RemoveMetaCallback, CreateMetaCallBack: CreateMetaCallBack}
}
