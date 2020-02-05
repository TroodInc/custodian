package transactions

type FileMetaDescriptionTransactionManager struct {
	dataManager *FileMetaDescriptionSyncer
}

func (fm *FileMetaDescriptionTransactionManager) BeginTransaction() (MetaDescriptionTransaction, error) {
	metaList, _, _ := fm.dataManager.List()
	return NewFileMetaDescriptionTransaction(Pending, metaList), nil
}

func (fm *FileMetaDescriptionTransactionManager) CommitTransaction(transaction MetaDescriptionTransaction) (error) {
	if transaction.State() == Pending {
		transaction.SetState(Committed)
		return nil
	} else {
		return &TransactionError{"MetaDescription driver is not in pending state"}
	}

}

func (fm *FileMetaDescriptionTransactionManager) RollbackTransaction(transaction MetaDescriptionTransaction) (error) {
	if transaction.State() != Pending {
		return &TransactionError{"MetaDescription driver is not in pending state"}
	}
	//remove created meta
	for _, metaName := range transaction.CreatedMetaNameList() {
		fm.dataManager.Remove(metaName)
	}
	//restore initial state
	for _, metaDescription := range transaction.InitialMetaList() {
		fm.dataManager.Create(transaction, metaDescription["name"].(string), metaDescription)
	}
	transaction.SetState(RolledBack)
	return nil
}

func NewFileMetaDescriptionTransactionManager(metaDescriptionSyncer *FileMetaDescriptionSyncer) *FileMetaDescriptionTransactionManager {
	return &FileMetaDescriptionTransactionManager{dataManager: metaDescriptionSyncer}
}
