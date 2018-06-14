package pg

import (
	"database/sql"
	"fmt"
	"logger"
	"server/meta"
)

type Syncer struct {
	db *sql.DB
}

/*
Example of the db info:
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

func (syncer *Syncer) CreateObj(m *meta.Meta) error {
	var md *MetaDDL
	var e error
	if md, e = MetaDDLFromMeta(m); e != nil {
		return e
	}
	var ds DDLStmts
	if ds, e = md.CreateScript(); e != nil {
		return e
	}
	for _, st := range ds {
		logger.Debug("Creating object in DB: %syncer\n", st.Code)
		if _, e := syncer.db.Exec(st.Code); e != nil {
			return &DDLError{table: m.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error())}
		}
	}
	return nil
}

func (syncer *Syncer) RemoveObj(name string, force bool) error {
	var md *MetaDDL
	var e error
	if md, e = MetaDDLFromDB(syncer.db, name); e != nil {
		return e
	}
	var ds DDLStmts
	if ds, e = md.DropScript(force); e != nil {
		return e
	}
	for _, st := range ds {
		logger.Debug("Removing object from DB: %syncer\n", st.Code)
		if _, e := syncer.db.Exec(st.Code); e != nil {
			return &DDLError{table: name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error())}
		}
	}
	return nil
}

//Update an existing business object
func (syncer *Syncer) UpdateObj(currentBusinessObj, newBusinessObject *meta.Meta) error {
	var currentBusinessObjMeta, newBusinessObjMeta *MetaDDL
	var err error
	if currentBusinessObjMeta, err = MetaDDLFromMeta(currentBusinessObj); err != nil {
		return err
	}
	if newBusinessObjMeta, err = MetaDDLFromMeta(newBusinessObject); err != nil {
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
		if _, e := syncer.db.Exec(ddlStatement.Code); e != nil {
			return &DDLError{table: currentBusinessObj.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", ddlStatement.Name, e.Error())}
		}
	}
	return nil
}

//Calculates the difference between the given and the existing business object in the database
func (syncer *Syncer) diffScripts(metaObj *meta.Meta) (DDLStmts, error) {
	newMetaDdl, e := MetaDDLFromMeta(metaObj)
	if e != nil {
		return nil, e
	}

	if metaDdlFromDB, err := MetaDDLFromDB(syncer.db, metaObj.Name); err == nil {
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

func (syncer *Syncer) UpdateObjTo(businessObject *meta.Meta) error {
	ddlStatements, e := syncer.diffScripts(businessObject)
	if e != nil {
		return e
	}
	for _, st := range ddlStatements {
		logger.Debug("Updating object in DB: %syncer\n", st.Code)
		if _, e := syncer.db.Exec(st.Code); e != nil {
			return &DDLError{table: businessObject.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%syncer': %syncer", st.Name, e.Error())}
		}
	}
	return nil
}

//Check if the given business object equals to the corresponding one stored in the database.
//The validation fails if the given business object is different
func (syncer *Syncer) ValidateObj(businessObject *meta.Meta) (bool, error) {
	ddlStatements, e := syncer.diffScripts(businessObject)
	if e != nil {
		return false, e
	} else {
		if len(ddlStatements) == 0 {
			return true, nil
		} else {
			return false, &meta.ValidationError{Message: "Inconsistent object state found."}
		}
	}
	return len(ddlStatements) == 0, nil
}
