package operations

import (
	"server/object/meta"
	"server/transactions"
)

type MigrationOperation interface {
	SyncDbDescription() []string
	SyncMetaDescription(*meta.Meta, transactions.MetaDescriptionTransaction, meta.MetaDescriptionSyncer) (*meta.Meta, error)
}
