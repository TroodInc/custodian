package pg

import (
	"database/sql"
	"fmt"
	"log"
	"custodian/logger"
	"regexp"
	"custodian/server/errors"
	"custodian/server/object/description"
	"custodian/server/object/meta"
	"custodian/server/transactions"
	"time"

	"github.com/lib/pq"
	"github.com/xo/dburl"
)

type Syncer struct {
	db *sql.DB
}

func getDBConnection(dbInfo string) *sql.DB {
	db, err := dburl.Open(dbInfo)
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

func (syncer *Syncer) NewDataManager() (*DataManager, error) {
	return NewDataManager(syncer.db)
}

func (syncer *Syncer) CreateObj(transaction transactions.DbTransaction, metaDescription *description.MetaDescription, descriptionSyncer meta.MetaDescriptionSyncer) error {
	tx := transaction.Transaction().(*sql.Tx)
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
	for _, st := range ds {
		logger.Debug("Creating object in DB: %syncer\n", st.Code)
		if _, e := tx.Exec(st.Code); e != nil {
			return errors.NewValidationError(ErrExecutingDDL, e.Error(), nil)
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
func (syncer *Syncer) UpdateObj(transaction transactions.DbTransaction, currentMetaDescription *description.MetaDescription, newMetaDescription *description.MetaDescription, descriptionSyncer meta.MetaDescriptionSyncer) error {
	tx := transaction.(*PgTransaction)
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
	for _, ddlStatement := range ddlStatements {
		logger.Debug("Updating object in DB: %s\n", ddlStatement.Code)
		if _, e := tx.Exec(ddlStatement.Code); e != nil {
			// TODO: Postgres error must return column field
			// TOFIX: https://github.com/postgres/postgres/blob/14751c340754af9f906a893eb87a894dea3adbc9/src/backend/commands/tablecmds.c#L10539
			var data map[string]interface{}
			if e.(*pq.Error).Code == "42804" {
				matched := regexp.MustCompile(`column "(.*)"`).FindAllStringSubmatch(e.(*pq.Error).Message, -1)
				if len(matched) > 0 {
					data = map[string]interface{}{"column": matched[0][1]}
				}
			}
			return errors.NewValidationError(ErrExecutingDDL, e.Error(), data)
		}
	}
	return nil
}

//Calculates the difference between the given and the existing business object in the database
func (syncer *Syncer) diffScripts(transaction transactions.DbTransaction, metaDescription *description.MetaDescription, descriptionSyncer meta.MetaDescriptionSyncer) (DdlStatementSet, error) {
	tx := transaction.(*PgTransaction)
	metaDdlFactory := NewMetaDdlFactory(descriptionSyncer)
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

func (syncer *Syncer) UpdateObjTo(transaction transactions.DbTransaction, metaDescription *description.MetaDescription, descriptionSyncer meta.MetaDescriptionSyncer) error {
	tx := transaction.(*PgTransaction)
	ddlStatements, e := syncer.diffScripts(tx, metaDescription, descriptionSyncer)
	if e != nil {
		return e
	}
	for _, st := range ddlStatements {
		logger.Debug("Updating object in DB: %s\n", st.Code)
		if _, e := tx.Exec(st.Code); e != nil {
			return errors.NewValidationError(ErrExecutingDDL, e.Error(), nil)
		}
	}
	return nil
}

//Check if the given business object equals to the corresponding one stored in the database.
//The validation fails if the given business object is different
func (syncer *Syncer) ValidateObj(transaction transactions.DbTransaction, metaDescription *description.MetaDescription, descriptionSyncer meta.MetaDescriptionSyncer) (bool, error) {
	ddlStatements, e := syncer.diffScripts(transaction, metaDescription, descriptionSyncer)
	if e != nil {
		return false, e
	} else {
		if len(ddlStatements) == 0 {
			return true, nil
		} else {
			return false, &description.ValidationError{Message: "Inconsistent object state found."}
		}
	}
	return len(ddlStatements) == 0, nil
}

//transaction related methods
func (syncer *Syncer) BeginTransaction() (transactions.DbTransaction, error) {
	tx, err := syncer.db.Begin()
	return &PgTransaction{Tx: tx}, err
}

func (syncer *Syncer) CommitTransaction(transaction transactions.DbTransaction) error {
	tx := transaction.(*PgTransaction)
	return tx.Commit()
}

func (syncer *Syncer) RollbackTransaction(transaction transactions.DbTransaction) error {
	tx := transaction.(*PgTransaction)
	err := tx.Rollback()
	return err
}

//
