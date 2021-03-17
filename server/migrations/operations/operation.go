package operations

import (
	"custodian/server/object"
	"custodian/server/object/description"
	"custodian/server/transactions"
)

type MigrationOperation interface {
	SyncDbDescription(metaToApply *description.MetaDescription, dbTransaction transactions.DbTransaction, metaDescriptionSyncer object.MetaDescriptionSyncer) (err error)
	SyncMetaDescription(*description.MetaDescription, object.MetaDescriptionSyncer) (*description.MetaDescription, error)
}

type AbstractMigrationOperation interface {
	SyncMetaDescription(*description.MetaDescription, object.MetaDescriptionSyncer) (*description.MetaDescription, error)
}
