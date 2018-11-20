package operations

import (
	"server/object/meta"
	"server/transactions"
)

type MigrationOperation interface {
	SyncDbDescription(*meta.Meta, transactions.DbTransaction) (err error)
	SyncMetaDescription(*meta.Meta, transactions.MetaDescriptionTransaction, meta.MetaDescriptionSyncer) (*meta.Meta, error)
}

type AbstractMigrationOperation interface {
	SyncMetaDescription(*meta.Meta, transactions.MetaDescriptionTransaction, meta.MetaDescriptionSyncer) (*meta.Meta, error)
}
