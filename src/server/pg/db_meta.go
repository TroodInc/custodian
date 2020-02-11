package pg

import (
	"database/sql"
	"server/object"
	"server/transactions"
)

type DbMetaDescriptionSyncer struct {
	DbTransactionManager transactions.DbTransactionManager
}

func NewDbMetaDescriptionSyncer(transactionManager transactions.DbTransactionManager) *DbMetaDescriptionSyncer {
	return &DbMetaDescriptionSyncer{transactionManager}
}

func (dm *DbMetaDescriptionSyncer) List() ([]map[string]interface{}, bool, error) {
	var metaList = make([]map[string]interface{}, 0)
	return metaList, false, nil
}

func (dm *DbMetaDescriptionSyncer) Get(name string) (map[string]interface{}, bool, error) {

	transaction, _ := dm.DbTransactionManager.BeginTransaction()

	ddl, _ := MetaDDLFromDB(transaction.Transaction().(*sql.Tx), name)

	metaObj := object.Meta{Name: name, Key: ddl.Pk}

	for _, col := range ddl.Columns {
		metaObj.Fields = append(metaObj.Fields, &object.Field{
			Name: col.Name,
			Type: col.Typ,
			Optional: col.Optional,
			Unique: col.Unique,
		})
	}

	dm.DbTransactionManager.CommitTransaction(transaction)


	return metaObj.ForExport(), true, nil
}

func (dm *DbMetaDescriptionSyncer) Create(transaction transactions.MetaDescriptionTransaction, name string, m map[string]interface{}) error {
	return nil
}

func (dm *DbMetaDescriptionSyncer) Remove(name string) (bool, error) {
	return false, nil
}

func (dm *DbMetaDescriptionSyncer) Update(name string, m map[string]interface{}) (bool, error) {
	return false, nil
}