package operations

import (
	"server/object"
	"server/transactions"
)

type MigrationOperation interface {
	SyncDbDescription(metaToApply *object.Meta, dbTransaction transactions.DbTransaction, metaDescriptionSyncer object.MetaDescriptionSyncer) (err error)
	SyncMetaDescription(*object.Meta, transactions.MetaDescriptionTransaction, object.MetaDescriptionSyncer) (*object.Meta, error)
}

type AbstractMigrationOperation interface {
	SyncMetaDescription(*object.Meta, transactions.MetaDescriptionTransaction, object.MetaDescriptionSyncer) (*object.Meta, error)
}
