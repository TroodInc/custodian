package pg

import (
	"database/sql"
	"fmt"
	"logger"
	"server/meta"
	"server/transactions"
)

type Syncer struct {
	db *sql.DB
}

/*
Example of the Tx info:
    - user=%s password=%s dbname=%s sslmode=disable
    - user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full
*/
func NewSyncer(dbInfo string) (*Syncer, error) {
	db, err := sql.Open("postgres", dbInfo)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Syncer{db: db}, nil
}

func (syncer *Syncer) Close() error {
	return syncer.db.Close()
}

func (syncer *Syncer) NewDataManager() (*DataManager, error) {
	return NewDataManager(syncer.db)
}

func (syncer *Syncer) CreateObj(transaction transactions.DbTransaction, m *object.Meta) error {
	tx := transaction.(*Tx)
	if err := syncer.ensureTransactionBegun(tx); err != nil {
		return err
	}
	var md *MetaDDL
	var e error
	metaDdlFactory := MetaDdlFactory{}
	if md, e = metaDdlFactory.Factory(m); e != nil {
		return e
	}
	var ds DDLStmts
	if ds, e = md.CreateScript(); e != nil {
		return e
	}
	for _, st := range ds {
		logger.Debug("Creating object in DB: %syncer\n", st.Code)
		if _, e := tx.Exec(st.Code); e != nil {
			return &DDLError{table: m.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error())}
		}
	}
	return nil
}

func (syncer *Syncer) RemoveObj(transaction transactions.DbTransaction, name string, force bool) error {
	tx := transaction.(*Tx)
	if err := syncer.ensureTransactionBegun(tx); err != nil {
		return err
	}
	var metaDdlFromDb *MetaDDL
	var e error
	if metaDdlFromDb, e = MetaDDLFromDB(tx.Tx, name); e != nil {
		return e
	}
	var ds DDLStmts
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
func (syncer *Syncer) UpdateObj(transaction transactions.DbTransaction, currentBusinessObj *object.Meta, newBusinessObject *object.Meta) error {
	tx := transaction.(*Tx)
	if err := syncer.ensureTransactionBegun(tx); err != nil {
		return err
	}
	var currentBusinessObjMeta, newBusinessObjMeta *MetaDDL
	var err error

	metaDdlFactory := MetaDdlFactory{}
	if currentBusinessObjMeta, err = metaDdlFactory.Factory(currentBusinessObj); err != nil {
		return err
	}

	if newBusinessObjMeta, err = metaDdlFactory.Factory(newBusinessObject); err != nil {
		return err
	}
	var metaDdlDiff *MetaDDLDiff
	if metaDdlDiff, err = currentBusinessObjMeta.Diff(newBusinessObjMeta); err != nil {
		return err
	}
	var ddlStatements DDLStmts
	if ddlStatements, err = metaDdlDiff.Script(); err != nil {
		return err
	}
	for _, ddlStatement := range ddlStatements {
		logger.Debug("Updating object in DB: %syncer\n", ddlStatement.Code)
		if _, e := tx.Exec(ddlStatement.Code); e != nil {
			return &DDLError{table: currentBusinessObj.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", ddlStatement.Name, e.Error())}
		}
	}
	return nil
}

//Calculates the difference between the given and the existing business object in the database
func (syncer *Syncer) diffScripts(transaction transactions.DbTransaction, metaObj *object.Meta) (DDLStmts, error) {
	tx := transaction.(*Tx)
	metaDdlFactory := MetaDdlFactory{}
	newMetaDdl, e := metaDdlFactory.Factory(metaObj)
	if e != nil {
		return nil, e
	}

	if metaDdlFromDB, err := MetaDDLFromDB(tx.Tx, metaObj.Name); err == nil {
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

func (syncer *Syncer) UpdateObjTo(transaction transactions.DbTransaction, businessObject *object.Meta) error {
	tx := transaction.(*Tx)
	if err := syncer.ensureTransactionBegun(tx); err != nil {
		return err
	}
	ddlStatements, e := syncer.diffScripts(tx, businessObject)
	if e != nil {
		return e
	}
	for _, st := range ddlStatements {
		logger.Debug("Updating object in DB: %syncer\n", st.Code)
		if _, e := tx.Exec(st.Code); e != nil {
			return &DDLError{table: businessObject.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%syncer': %syncer", st.Name, e.Error())}
		}
	}
	return nil
}

//Check if the given business object equals to the corresponding one stored in the database.
//The validation fails if the given business object is different
func (syncer *Syncer) ValidateObj(transaction transactions.DbTransaction, businessObject *object.Meta) (bool, error) {
	tx := transaction.(*Tx)
	if err := syncer.ensureTransactionBegun(tx); err != nil {
		return false, err
	}
	ddlStatements, e := syncer.diffScripts(tx, businessObject)
	if e != nil {
		return false, e
	} else {
		if len(ddlStatements) == 0 {
			return true, nil
		} else {
			return false, &object.ValidationError{Message: "Inconsistent object state found."}
		}
	}
	return len(ddlStatements) == 0, nil
}

//transaction related methods
func (syncer *Syncer) BeginTransaction() (transactions.DbTransaction, error) {
	tx, err := syncer.db.Begin()
	return &Tx{Tx: tx}, err
}

func (syncer *Syncer) CommitTransaction(transaction transactions.DbTransaction) (error) {
	tx := transaction.(*Tx)
	return tx.Commit()
}

func (syncer *Syncer) RollbackTransaction(transaction transactions.DbTransaction) (error) {
	tx := transaction.(*Tx)
	err := tx.Rollback()
	return err
}

func (syncer *Syncer) ensureTransactionBegun(transaction transactions.DbTransaction) (error) {
	tx := transaction.(*Tx)
	if tx == nil {
		return &TransactionNotBegunError{}
	}
	return nil
}

//
