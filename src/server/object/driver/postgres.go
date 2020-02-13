package driver

import (
	"database/sql"
	"fmt"
	"logger"
	"server/object/meta"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
)

type PostgresDriver struct {
	transactionManager 	 transactions.DbTransactionManager
}

func NewPostgresDriver(dbConnectionUrl string) *PostgresDriver {
	mds, _ := pg.NewSyncer(dbConnectionUrl)
	dm, _ := mds.NewDataManager()
	dtm := pg_transactions.NewPgDbTransactionManager(dm)
	return &PostgresDriver{
		transactionManager: dtm,
	}
}

func (p *PostgresDriver) Get(name string) *meta.Meta {
	transaction, _ := p.transactionManager.BeginTransaction()

	ddl, _ := pg.MetaDDLFromDB(transaction.Transaction().(*sql.Tx), name)

	metaObj := &meta.Meta{Name: name, Key: ddl.Pk}

	for _, col := range ddl.Columns {
		metaObj.AddField(&meta.Field{
			Name: col.Name,
			Type: col.Typ,
			Optional: col.Optional,
			Unique: col.Unique,
		})
	}

	p.transactionManager.CommitTransaction(transaction)


	return metaObj
}

func (p *PostgresDriver) List() ([]*meta.Meta, error) {
	return nil, nil
}

func (p *PostgresDriver) Create(object *meta.Meta) error {
	return nil
}

func (p *PostgresDriver) Update(*meta.Meta) (bool, error){
	return false, nil
}

func (p *PostgresDriver) Remove(name string) (bool, error) {
	transaction, _ := p.transactionManager.BeginTransaction()
	tx := transaction.(*pg.PgTransaction)
	metaDdlFromDb, e := pg.MetaDDLFromDB(tx.Tx, name)
	if e != nil {
		if e.(*pg.DDLError).Code() == meta.ErrNotFound {
			return true, nil
		}
		return false, e
	}
	var ds pg.DdlStatementSet
	if ds, e = metaDdlFromDb.DropScript(true); e != nil {
		return false, e
	}
	for _, st := range ds {
		logger.Debug("Removing object from DB: %syncer\n", st.Code)
		if _, e := tx.Exec(st.Code); e != nil {
			return false, pg.NewDdlError(
				pg.ErrExecutingDDL, fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error()), name,
			)
		}
	}
	return true, nil
}