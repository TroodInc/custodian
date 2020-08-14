package operations

import (
	"custodian/server/object/meta"
	"custodian/server/transactions"
	"custodian/server/object/description"
)

type MigrationOperation interface {
	SyncDbDescription(metaToApply *description.MetaDescription, dbTransaction transactions.DbTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (err error)
	SyncMetaDescription(*description.MetaDescription, transactions.MetaDescriptionTransaction, meta.MetaDescriptionSyncer) (*description.MetaDescription, error)
}

type AbstractMigrationOperation interface {
	SyncMetaDescription(*description.MetaDescription, transactions.MetaDescriptionTransaction, meta.MetaDescriptionSyncer) (*description.MetaDescription, error)
}
