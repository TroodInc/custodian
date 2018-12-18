package migrations

import (
	"server/object/meta"
	"server/transactions"
	"server/migrations/migrations"
)

type MigrationRunner struct {
}

func (mr *MigrationRunner) Run(migration *migrations.Migration, metaObj *meta.Meta, globalTransaction *transactions.GlobalTransaction) (processedMetaObj *meta.Meta, err error) {
	//for _, operation := range migration.Operations {
	//	//processedMetaObj, err = operation.ProcessMetaMigration(metaObj, globalTransaction.MetaDescriptionTransaction)
	//	//if err != nil {
	//	//	return nil, err
	//	//} else {
	//	//}
	//}
	return processedMetaObj, err
}
