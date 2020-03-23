package driver

import (
	"database/sql"
	"fmt"
	"logger"
	"server/errors"
	"server/object/meta"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
)

const TableNamePrefix = "o_"

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

func (p *PostgresDriver) Get(name string) map[string]interface{} {
	metaMap := p.getMetaDescription(name)
	if metaMap != nil {
		metaMap["actions"] = p.getActions(name)
	}

	return metaMap
}

func (p *PostgresDriver) List() ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, 0)
	for _, name := range p.getMetaList() {
		if m := p.Get(name); m != nil {
			result = append(result, m)
		}
	}

	return result, nil
}

func (p *PostgresDriver) Create(object *meta.Meta) error {
	transaction, _ := p.transactionManager.BeginTransaction()
	tx := transaction.Transaction().(*sql.Tx)
	var md *pg.MetaDDL
	var e error
	metaDdlFactory := pg.NewMetaDdlFactory()
	if md, e = metaDdlFactory.Factory(object); e != nil {
		p.transactionManager.RollbackTransaction(transaction)
		return e
	}
	var ds pg.DdlStatementSet
	if ds, e = md.CreateScript(); e != nil {
		p.transactionManager.RollbackTransaction(transaction)
		return e
	}
	for _, st := range ds {
		logger.Debug("Creating object in DB: %syncer\n", st.Code)
		if _, e := tx.Exec(st.Code); e != nil {
			p.transactionManager.RollbackTransaction(transaction)
			return errors.NewFatalError(
				"", fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error()), nil,
			)
		}
	}
	p.transactionManager.CommitTransaction(transaction)
	return nil
}

func (p *PostgresDriver) Update(object *meta.Meta) (bool, error){
	transaction, _ := p.transactionManager.BeginTransaction()
	tx := transaction.(*pg.PgTransaction)
	var currentBusinessObjMeta, newBusinessObjMeta *pg.MetaDDL
	var err error

	metaDdlFactory := pg.NewMetaDdlFactory()
	if currentBusinessObjMeta, err = metaDdlFactory.Factory(object); err != nil {
		p.transactionManager.RollbackTransaction(transaction)
		return false, err
	}

	if newBusinessObjMeta, err = metaDdlFactory.Factory(object); err != nil {
		p.transactionManager.RollbackTransaction(transaction)
		return false, err
	}
	var metaDdlDiff *pg.MetaDDLDiff
	if metaDdlDiff, err = currentBusinessObjMeta.Diff(newBusinessObjMeta); err != nil {
		p.transactionManager.RollbackTransaction(transaction)
		return false, err
	}
	var ddlStatements pg.DdlStatementSet
	if ddlStatements, err = metaDdlDiff.Script(); err != nil {
		p.transactionManager.RollbackTransaction(transaction)
		return false, err
	}
	for _, ddlStatement := range ddlStatements {
		logger.Debug("Updating object in DB: %syncer\n", ddlStatement.Code)
		if _, e := tx.Exec(ddlStatement.Code); e != nil {
			p.transactionManager.RollbackTransaction(transaction)
			return false, errors.NewFatalError(
				"",fmt.Sprintf("Error while executing statement '%s': %s", ddlStatement.Name, e.Error()), nil,
			)
		}
	}
	return true, nil
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

func (p *PostgresDriver) getActions(name string) []map[string]interface{} {
	SqlGetMetaActions := fmt.Sprintf(`
		SELECT * 
		FROM system_actions
		WHERE meta = '%s'`, name)

	transaction, _ := p.transactionManager.BeginTransaction()
	tx := transaction.(*pg.PgTransaction)

	result := make([]map[string]interface{}, 0)
	if rows, err := tx.Query(SqlGetMetaActions); err == nil {
		for rows.Next() {

		}
	}

	p.transactionManager.CommitTransaction(transaction)

	return result
}

func (p *PostgresDriver) getMetaList() []string {
	SqlGetTablesList := fmt.Sprintf(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema='public' 
		  AND table_type='BASE TABLE' 
		  AND table_name LIKE '%s%%';`,TableNamePrefix)

	transaction, _ := p.transactionManager.BeginTransaction()
	tx := transaction.(*pg.PgTransaction)

	var result = make([]string, 0)
	if rows, err := tx.Query(SqlGetTablesList); err == nil {
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err == nil {
				result = append(result, name[len(TableNamePrefix):])
			}
		}
	} else {
		fmt.Println(err.Error())
	}

	p.transactionManager.CommitTransaction(transaction)

	return result
}

func (p *PostgresDriver) getMetaDescription(name string) map[string]interface{} {
	SqlGetTableSchema := fmt.Sprintf(`
		SELECT
			f.attnum AS number,
			f.attname AS name,
			f.attnum,
			f.attnotnull AS notnull,
			pg_catalog.format_type(f.atttypid,f.atttypmod) AS type,
			CASE
				WHEN p.contype = 'p' THEN 't'
				ELSE 'f'
				END AS primarykey,
			CASE
				WHEN p.contype = 'u' THEN 't'
				ELSE 'f'
				END AS uniquekey,
			CASE
				WHEN p.contype = 'f' THEN g.relname
				END AS foreignkey,
			CASE
				WHEN p.contype = 'f' THEN p.confkey
				END AS foreignkey_fieldnum,
			CASE
				WHEN p.contype = 'f' THEN g.relname
				END AS foreignkey,
			CASE
				WHEN p.contype = 'f' THEN p.conkey
				END AS foreignkey_connnum,
			CASE
				WHEN f.atthasdef = 't' THEN d.adsrc
				END AS default
		FROM pg_attribute f
				 JOIN pg_class c ON c.oid = f.attrelid
				 JOIN pg_type t ON t.oid = f.atttypid
				 LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
				 LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
				 LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)
				 LEFT JOIN pg_class AS g ON p.confrelid = g.oid
		WHERE c.relkind = 'r'::char
		  AND n.nspname = 'public'  -- Replace with Schema name
		  AND c.relname = '%s%s'  -- Replace with table name
		  AND f.attnum > 0 ORDER BY number;`, TableNamePrefix, name)

	transaction, _ := p.transactionManager.BeginTransaction()
	tx := transaction.(*pg.PgTransaction)

	if rows, err := tx.Query(SqlGetTableSchema); err == nil {
		result := map[string]interface{}{
			"name": name,
			"fields": make(map[string]interface{}, 0),
		}
		for rows.Next() {
			var f struct{
				Number, Attnum int
				InForeignKeyFieldNum, OutForeignKeyConnNum []int
				NotNull, PrimaryKey, UniqueKey bool
				Name, Type, InForeignKey, OutForeignKey, Default string
			}

			if err := rows.Scan(
				&f.Number, &f.Name, &f.Attnum, &f.NotNull, &f.Type, &f.PrimaryKey, &f.UniqueKey,
				&f.InForeignKey, &f.InForeignKeyFieldNum, &f.OutForeignKey, &f.OutForeignKeyConnNum, &f.Default,
			); err == nil {
				if f.PrimaryKey { result["key"] = f.Name }

				result["fields"].(map[string]interface{})[f.Name] = map[string]interface{}{
					"name": f.Name, "type": f.Type, "default": f.Default,
				}
			}
		}
		p.transactionManager.CommitTransaction(transaction)
		return result
	}

	p.transactionManager.RollbackTransaction(transaction)
	return nil
}