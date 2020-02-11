package operations

import (
	"server/object/meta"
	"server/transactions"
)

type MigrationOperation interface {
	SyncDbDescription(metaToApply *meta.Meta, dbTransaction transactions.DbTransaction, metaDescriptionSyncer meta.MetaDescriptionSyncer) (err error)
	SyncMetaDescription(*meta.Meta, transactions.MetaDescriptionTransaction, meta.MetaDescriptionSyncer) (*meta.Meta, error)
}

type AbstractMigrationOperation interface {
	SyncMetaDescription(*meta.Meta, transactions.MetaDescriptionTransaction, meta.MetaDescriptionSyncer) (*meta.Meta, error)
}
