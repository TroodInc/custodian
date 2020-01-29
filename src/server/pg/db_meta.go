package pg

import (
	"database/sql"
	"server/object/description"
	"server/object/v2_meta"
	"server/transactions"
)

type DbMetaDescriptionSyncer struct {
	DbTransactionManager  transactions.DbTransactionManager
}

func NewDbMetaDescriptionSyncer(transactionManager transactions.DbTransactionManager) *DbMetaDescriptionSyncer {
	return &DbMetaDescriptionSyncer{transactionManager}
}

func (dm *DbMetaDescriptionSyncer) List() ([]*description.MetaDescription, bool, error) {
	var metaList = make([]*description.MetaDescription, 0)
	return metaList, false, nil
}

func (dm *DbMetaDescriptionSyncer) Get(name string) (*description.MetaDescription, bool, error) {

	transaction, _ := dm.DbTransactionManager.BeginTransaction()

	ddl, _ := MetaDDLFromDB(transaction.Transaction().(*sql.Tx), name)

	var meta = description.MetaDescription{Name: name, Key: ddl.Pk}

	for _, col := range ddl.Columns {
		meta.Fields = append(meta.Fields, description.Field{
			Name: col.Name,
			Type: col.Typ,
			Optional: col.Optional,
			Unique: col.Unique,
		})
	}

	dm.DbTransactionManager.CommitTransaction(transaction)
	return &meta, true, nil
}

func (dm *DbMetaDescriptionSyncer) Create(transaction transactions.MetaDescriptionTransaction, m description.MetaDescription) error {
	return nil
}

func (dm *DbMetaDescriptionSyncer) V2Create(transaction transactions.MetaDescriptionTransaction, m *v2_meta.V2Meta) error {
	return nil
}

func (dm *DbMetaDescriptionSyncer) Remove(name string) (bool, error) {
	return false, nil
}

func (dm *DbMetaDescriptionSyncer) Update(name string, m description.MetaDescription) (bool, error) {
	return false, nil
}