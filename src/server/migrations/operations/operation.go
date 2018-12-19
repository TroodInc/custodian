package operations

import (
	"server/object/meta"
	"server/transactions"
)

type MigrationOperation interface {
	SyncDbDescription(metaToApplyTo *meta.Meta, dbTransaction transactions.DbTransaction) (err error)
	SyncMetaDescription(*meta.Meta, transactions.MetaDescriptionTransaction, meta.MetaDescriptionSyncer) (*meta.Meta, error)
}

type AbstractMigrationOperation interface {
	SyncMetaDescription(*meta.Meta, transactions.MetaDescriptionTransaction, meta.MetaDescriptionSyncer) (*meta.Meta, error)
}
