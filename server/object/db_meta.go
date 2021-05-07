package object

import (
	"custodian/server/object/description"
	"custodian/server/transactions"

	"database/sql"
)

type DbMetaDescriptionSyncer struct {
	DbTransactionManager transactions.DbTransactionManager
	cache                *MetaCache
}

func NewDbMetaDescriptionSyncer(transactionManager transactions.DbTransactionManager) *DbMetaDescriptionSyncer {
	return &DbMetaDescriptionSyncer{transactionManager, NewCache()}
}

func (dm *DbMetaDescriptionSyncer) Cache() *MetaCache {
	return dm.cache
}

func (dm *DbMetaDescriptionSyncer) List() ([]*description.MetaDescription, bool, error) {
	var metaList = make([]*description.MetaDescription, 0)
	return metaList, false, nil
}

func (dm *DbMetaDescriptionSyncer) Get(name string) (*description.MetaDescription, bool, error) {

	transaction, err := dm.DbTransactionManager.BeginTransaction()
	if err != nil {
		return nil, false, err
	}

	ddl, err := MetaDDLFromDB(transaction.Transaction().(*sql.Tx), name)
	if err != nil {
		dm.DbTransactionManager.RollbackTransaction(transaction)
		return nil, false, err
	}

	var meta = description.MetaDescription{Name: name, Key: ddl.Pk}

	for _, col := range ddl.Columns {
		meta.Fields = append(meta.Fields, description.Field{
			Name:     col.Name,
			Type:     col.Typ,
			Optional: col.Optional,
			Unique:   col.Unique,
		})
	}

	dm.DbTransactionManager.CommitTransaction(transaction)
	return &meta, true, nil
}

func (dm *DbMetaDescriptionSyncer) Create(m description.MetaDescription) error {
	return nil
}

func (dm *DbMetaDescriptionSyncer) Remove(name string) (bool, error) {
	return false, nil
}

func (dm *DbMetaDescriptionSyncer) Update(name string, m description.MetaDescription) (bool, error) {
	return false, nil
}
