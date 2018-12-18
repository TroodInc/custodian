package migrations

import "server/transactions"

type MigrationHouseKeeper interface {
	EnsureHistoryTableExists(transaction transactions.DbTransaction)
}
