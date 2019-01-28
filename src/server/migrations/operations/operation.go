package operations

import (
	"server/object/meta"
	"server/transactions"
	"server/object/description"
)

type MigrationOperation interface {
	SyncDbDescription(metaToApply *description.MetaDescription, dbTransaction transactions.DbTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (err error)
	SyncMetaDescription(*description.MetaDescription, transactions.MetaDescriptionTransaction, meta.MetaDescriptionSyncer) (*description.MetaDescription, error)
}

type AbstractMigrationOperation interface {
	SyncMetaDescription(*description.MetaDescription, transactions.MetaDescriptionTransaction, meta.MetaDescriptionSyncer) (*description.MetaDescription, error)
}
