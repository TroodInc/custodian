package operations

import (
	"server/object"
	"server/object/meta"
	"server/transactions"
)

type MigrationOperation interface {
	SyncDbDescription(metaToApply *meta.Meta, dbTransaction transactions.DbTransaction) (err error)
	SyncMetaDescription(*meta.Meta, transactions.MetaDescriptionTransaction, *object.Store) (*meta.Meta, error)
}

type AbstractMigrationOperation interface {
	SyncMetaDescription(*meta.Meta, transactions.MetaDescriptionTransaction, *object.Store) (*meta.Meta, error)
}
