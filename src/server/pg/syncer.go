package pg

import (
	"database/sql"
	"fmt"
	"github.com/xo/dburl"
	"log"
	"logger"
	"server/errors"
	"server/object/meta"
	"server/transactions"
	"time"
)

type Syncer struct {
	db *sql.DB
}

func getDBConnection(dbInfo string) *sql.DB {
	db, err := dburl.Open(dbInfo)
	if err != nil {
		logger.Error("%", err)
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

func (syncer *Syncer) NewDataManager() (*DataManager, error) {
	return NewDataManager(syncer.db)
}

func (syncer *Syncer) CreateObj(transaction transactions.DbTransaction, metaDescription *meta.Meta) error {
	tx := transaction.Transaction().(*sql.Tx)
	var md *MetaDDL
	var e error
	metaDdlFactory := NewMetaDdlFactory()
	if md, e = metaDdlFactory.Factory(metaDescription); e != nil {
		return e
	}
	var ds DdlStatementSet
	if ds, e = md.CreateScript(); e != nil {
		return e
	}
	for _, st := range ds {
		logger.Debug("Creating object in DB: %syncer\n", st.Code)
		if _, e := tx.Exec(st.Code); e != nil {
			return &DDLError{table: metaDescription.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error())}
		}
	}
	return nil
}


func (syncer *Syncer) RemoveObj(transaction transactions.DbTransaction, name string, force bool) error {
	tx := transaction.(*PgTransaction)
	var metaDdlFromDb *MetaDDL
	var e error
	if metaDdlFromDb, e = MetaDDLFromDB(tx.Tx, name); e != nil {
		if e.(*DDLError).code == ErrNotFound {
			return nil
		}
		return e
	}
	var ds DdlStatementSet
	if ds, e = metaDdlFromDb.DropScript(force); e != nil {
		return e
	}
	for _, st := range ds {
		logger.Debug("Removing object from DB: %syncer\n", st.Code)
		if _, e := tx.Exec(st.Code); e != nil {
			return &DDLError{table: name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error())}
		}
	}
	return nil
}

//UpdateRecord an existing business object
func (syncer *Syncer) UpdateObj(transaction transactions.DbTransaction, currentMetaDescription *meta.Meta, newMetaDescription *meta.Meta) error {
	tx := transaction.(*PgTransaction)
	var currentBusinessObjMeta, newBusinessObjMeta *MetaDDL
	var err error

	metaDdlFactory := NewMetaDdlFactory()
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
	for _, ddlStatement := range ddlStatements {
		logger.Debug("Updating object in DB: %syncer\n", ddlStatement.Code)
		if _, e := tx.Exec(ddlStatement.Code); e != nil {
			return &DDLError{table: currentMetaDescription.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", ddlStatement.Name, e.Error())}
		}
	}
	return nil
}

//Calculates the difference between the given and the existing business object in the database
func (syncer *Syncer) diffScripts(transaction transactions.DbTransaction, metaDescription *meta.Meta) (DdlStatementSet, error) {
	tx := transaction.(*PgTransaction)
	metaDdlFactory := NewMetaDdlFactory()
	newMetaDdl, e := metaDdlFactory.Factory(metaDescription)
	if e != nil {
		return nil, e
	}

	if metaDdlFromDB, err := MetaDDLFromDB(tx.Tx, metaDescription.Name); err == nil {
		diff, err := metaDdlFromDB.Diff(newMetaDdl)
		if err != nil {
			return nil, err
		}
		return diff.Script()
	} else if ddlErr, ok := err.(*DDLError); ok && ddlErr.code == ErrNotFound {
		return newMetaDdl.CreateScript()
	} else {
		return nil, e
	}

}

func (syncer *Syncer) UpdateObjTo(transaction transactions.DbTransaction, metaDescription *meta.Meta) error {
	tx := transaction.(*PgTransaction)
	ddlStatements, e := syncer.diffScripts(tx, metaDescription)
	if e != nil {
		return e
	}
	for _, st := range ddlStatements {
		logger.Debug("Updating object in DB: %syncer\n", st.Code)
		if _, e := tx.Exec(st.Code); e != nil {
			return &DDLError{table: metaDescription.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%syncer': %syncer", st.Name, e.Error())}
		}
	}
	return nil
}

//Check if the given business object equals to the corresponding one stored in the database.
//The validation fails if the given business object is different
func (syncer *Syncer) ValidateObj(transaction transactions.DbTransaction, metaDescription *meta.Meta) (bool, error) {
	ddlStatements, e := syncer.diffScripts(transaction, metaDescription)
	if e != nil {
		return false, e
	} else {
		if len(ddlStatements) == 0 {
			return true, nil
		} else {
			// TODO: add error code
			return false, errors.NewValidationError("", "Inconsistent object state found.", nil)
		}
	}
	return len(ddlStatements) == 0, nil
}

//transaction related methods
func (syncer *Syncer) BeginTransaction() (transactions.DbTransaction, error) {
	tx, err := syncer.db.Begin()
	return &PgTransaction{Tx: tx}, err
}

func (syncer *Syncer) CommitTransaction(transaction transactions.DbTransaction) (error) {
	tx := transaction.(*PgTransaction)
	return tx.Commit()
}

func (syncer *Syncer) RollbackTransaction(transaction transactions.DbTransaction) (error) {
	tx := transaction.(*PgTransaction)
	err := tx.Rollback()
	return err
}

//
