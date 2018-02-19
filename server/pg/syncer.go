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

func (s *Syncer) Close() error {
	return s.db.Close()
}

func (s *Syncer) NewDataManager() (*DataManager, error) {
	return NewDataManager(s.db)
}

func (s *Syncer) CreateObj(m *meta.Meta) error {
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
		logger.Debug("Creating object in DB: %s\n", st.Code)
		if _, e := s.db.Exec(st.Code); e != nil {
			return &DDLError{table: m.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error())}
		}
	}
	return nil
}

func (s *Syncer) RemoveObj(name string) error {
	var md *MetaDDL
	var e error
	if md, e = MetaDDLFromDB(s.db, name); e != nil {
		return e
	}
	var ds DDLStmts
	if ds, e = md.DropScript(); e != nil {
		return e
	}
	for _, st := range ds {
		logger.Debug("Removing object from DB: %s\n", st.Code)
		if _, e := s.db.Exec(st.Code); e != nil {
			return &DDLError{table: name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error())}
		}
	}
	return nil
}

func (s *Syncer) UpdateObj(old, new *meta.Meta) error {
	var oMeta, nMeta *MetaDDL
	var e error
	if oMeta, e = MetaDDLFromMeta(old); e != nil {
		return e
	}
	if nMeta, e = MetaDDLFromMeta(new); e != nil {
		return e
	}
	var md *MetaDDLDiff
	if md, e = oMeta.Diff(nMeta); e != nil {
		return e
	}
	var ds DDLStmts
	if ds, e = md.Script(); e != nil {
		return e
	}
	for _, st := range ds {
		logger.Debug("Updating object in DB: %s\n", st.Code)
		if _, e := s.db.Exec(st.Code); e != nil {
			return &DDLError{table: old.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error())}
		}
	}
	return nil
}

func (s *Syncer) diffScripts(m *meta.Meta) (DDLStmts, error) {
	nMeta, e := MetaDDLFromMeta(m)
	if e != nil {
		return nil, e
	}

	if dbMeta, e := MetaDDLFromDB(s.db, m.Name); e == nil {
		diff, e := dbMeta.Diff(nMeta)
		if e != nil {
			return nil, e
		}
		return diff.Script()
	} else if ddlErr, ok := e.(*DDLError); ok && ddlErr.code == ErrNotFound {
		return nMeta.CreateScript()
	} else {
		return nil, e
	}

}

func (s *Syncer) UpdateObjTo(m *meta.Meta) error {
	stms, e := s.diffScripts(m)
	if e != nil {
		return e
	}
	for _, st := range stms {
		logger.Debug("Updating object in DB: %s\n", st.Code)
		if _, e := s.db.Exec(st.Code); e != nil {
			return &DDLError{table: m.Name, code: ErrExecutingDDL, msg: fmt.Sprintf("Error while executing statement '%s': %s", st.Name, e.Error())}
		}
	}
	return nil
}

func (s *Syncer) ValidateObj(m *meta.Meta) (bool, error) {
	stms, e := s.diffScripts(m)
	if e != nil {
		return false, e
	}
	return len(stms) == 0, nil
}
