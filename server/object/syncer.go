package object

import (
	"custodian/logger"
	"custodian/server/errors"
	"custodian/server/object/description"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/jackc/pgconn"
	_ "github.com/jackc/pgx/v4/stdlib" // needed for proper driver work
	"github.com/lib/pq"
)

type Syncer struct {
	db *sql.DB
}

func getDBConnection(dbInfo string) *sql.DB {
	db, err := sql.Open("pgx", dbInfo)
	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(50)
	db.SetMaxOpenConns(50)
	if err != nil {
		logger.Error("%s", err)
		logger.Error("Could not connect to Postgres.")

		return &sql.DB{}
	}

	return db
}

var activeDBConnection *sql.DB

func NewSyncer(dbInfo string) (*Syncer, error) {
	if activeDBConnection == nil {
		activeDBConnection = getDBConnection(dbInfo)
	}
	alive := activeDBConnection.Ping()

	for alive != nil {
		log.Print("Connection to Postgres was lost. Waiting for 5s...")
		activeDBConnection.Close()
		time.Sleep(5 * time.Second)
		log.Print("Reconnecting...")
		activeDBConnection = getDBConnection(dbInfo)
		alive = activeDBConnection.Ping()
	}

	return &Syncer{db: activeDBConnection}, nil
}

func (syncer *Syncer) Close() error {
	return syncer.db.Close()
}

func (syncer *Syncer) NewDataManager() (*DBManager, error) {
	return NewDataManager(syncer.db)
}

func (syncer *Syncer) CreateObj(globalTransactionManager *PgDbTransactionManager, metaDescription *description.MetaDescription, descriptionSyncer MetaDescriptionSyncer) error {
	var md *MetaDDL
	var e error
	metaDdlFactory := NewMetaDdlFactory(descriptionSyncer)
	if md, e = metaDdlFactory.Factory(metaDescription); e != nil {
		return e
	}
	var ds DdlStatementSet
	if ds, e = md.CreateScript(); e != nil {
		return e
	}
	transaction, err := globalTransactionManager.BeginTransaction()
	if err != nil {
		globalTransactionManager.RollbackTransaction(transaction)
		return nil
	}
	tx := transaction.Transaction().(*sql.Tx)
	for _, st := range ds {
		logger.Debug("Creating object in DB: %syncer\n", st.Code)
		if _, e := tx.Exec(st.Code); e != nil {
			globalTransactionManager.RollbackTransaction(transaction)
			return errors.NewValidationError(ErrExecutingDDL, e.Error(), nil)
		}
	}
	globalTransactionManager.CommitTransaction(transaction)
	return nil
}

func (syncer *Syncer) RemoveObj(globalTransactionManager *PgDbTransactionManager, name string, force bool) error {
	transaction, err := globalTransactionManager.BeginTransaction()
	if err != nil {
		globalTransactionManager.RollbackTransaction(transaction)
		return err
	}
	tx := transaction.Transaction().(*sql.Tx)
	var metaDdlFromDb *MetaDDL
	var e error
	if metaDdlFromDb, e = MetaDDLFromDB(tx, name); e != nil {
		if e.(*DDLError).code == ErrNotFound {
			globalTransactionManager.RollbackTransaction(transaction)
			return nil
		}
		globalTransactionManager.RollbackTransaction(transaction)
		return e
	}
	var ds DdlStatementSet
	if ds, e = metaDdlFromDb.DropScript(force); e != nil {
		globalTransactionManager.RollbackTransaction(transaction)
		return e
	}
	for _, st := range ds {
		logger.Debug("Removing object from DB: %syncer\n", st.Code)
		if _, e := tx.Exec(st.Code); e != nil {
			globalTransactionManager.RollbackTransaction(transaction)
			return &DDLError{table: name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error())}
		}
	}
	globalTransactionManager.CommitTransaction(transaction)

	return nil
}

//UpdateRecord an existing business object
func (syncer *Syncer) UpdateObj(globalTransactionManager *PgDbTransactionManager, currentMetaDescription *description.MetaDescription, newMetaDescription *description.MetaDescription, descriptionSyncer MetaDescriptionSyncer) error {
	var currentBusinessObjMeta, newBusinessObjMeta *MetaDDL
	var err error

	metaDdlFactory := NewMetaDdlFactory(descriptionSyncer)
	if currentBusinessObjMeta, err = metaDdlFactory.Factory(currentMetaDescription); err != nil {
		return err
	}

	if newBusinessObjMeta, err = metaDdlFactory.Factory(newMetaDescription); err != nil {
		return err
	}
	var metaDdlDiff *MetaDDLDiff
	if metaDdlDiff, err = currentBusinessObjMeta.Diff(newBusinessObjMeta); err != nil {
		return err
	}
	var ddlStatements DdlStatementSet
	if ddlStatements, err = metaDdlDiff.Script(); err != nil {
		return err
	}
	transaction, err := globalTransactionManager.BeginTransaction()
	if err != nil {
		globalTransactionManager.RollbackTransaction(transaction)
		return err
	}
	tx := transaction.Transaction().(*sql.Tx)
	for _, ddlStatement := range ddlStatements {
		logger.Debug("Updating object in DB: %s\n", ddlStatement.Code)
		if _, e := tx.Exec(ddlStatement.Code); e != nil {
			// TODO: Postgres error must return column field
			// TOFIX: https://github.com/postgres/postgres/blob/14751c340754af9f906a893eb87a894dea3adbc9/src/backend/commands/tablecmds.c#L10539
			globalTransactionManager.RollbackTransaction(transaction)
			var data map[string]interface{}
			if e.(*pgconn.PgError).Code == "42804" {
				matched := regexp.MustCompile(`column "(.*)"`).FindAllStringSubmatch(e.(*pq.Error).Message, -1)
				if len(matched) > 0 {
					data = map[string]interface{}{"column": matched[0][1]}
				}
			}
			return errors.NewValidationError(ErrExecutingDDL, e.Error(), data)
		}
	}
	globalTransactionManager.CommitTransaction(transaction)
	return nil
}
