package object

import (
	"custodian/logger"
	"custodian/server/errors"
	"custodian/server/object/description"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	SQL_CREATE_META_TABLE string = `CREATE TABLE IF NOT EXISTS o___meta__ (
		id serial NOT NULL PRIMARY KEY,
		meta_description json NOT NULL
	);`
	SQL_GET_META_LIST     string = `SELECT meta_description FROM o___meta__ ORDER BY id;`
	GET_META_OBJ          string = `SELECT meta_description FROM o___meta__ WHERE meta_description ->> 'name'=$1;`
	DELETE_META_OBJ       string = `DELETE FROM o___meta__ WHERE meta_description ->> 'name'=$1;`
	CHECK_META_OBJ_EXISTS string = `SELECT 1 FROM o___meta__ WHERE meta_description ->> 'name'=$1;`
	CREATE_META_OBJ       string = `INSERT INTO o___meta__ (meta_description) VALUES ($1);`
	UPDATE_META_OBJ       string = `UPDATE o___meta__ SET meta_description=$1 WHERE meta_description ->> 'name'=$2;`
)

func checkMetaObjExists(name string, tx *sql.Tx) (bool, error) {
	var exists bool
	if err := tx.QueryRow(CHECK_META_OBJ_EXISTS, name).Scan(&exists); err != nil {
		if err != sql.ErrNoRows {
			return exists, err
		}
	}
	return exists, nil
}

func getMetaObjFromDb(name string, tx *sql.Tx) (description.MetaDescription, error) {
	var metaObj string
	var meta = description.MetaDescription{}

	if err := tx.QueryRow(GET_META_OBJ, name).Scan(&metaObj); err != nil {
		if err != sql.ErrNoRows {
			return meta, err
		}

	}

	jd := json.NewDecoder(strings.NewReader(metaObj))
	if err := jd.Decode(&meta); err != nil {
		return meta, err
	}

	return meta, nil
}

func (md *PgMetaDescriptionSyncer) CreateMetaTableIfNotExists() {
	globalTransaction, err := md.globalTransactionManager.BeginTransaction()
	if err != nil {
		return
	}
	tx := globalTransaction.Transaction().(*sql.Tx)
	_, err = tx.Exec(SQL_CREATE_META_TABLE)
	if err != nil {
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return
	}
	md.globalTransactionManager.CommitTransaction(globalTransaction)
}

type PgMetaDescriptionSyncer struct {
	globalTransactionManager *PgDbTransactionManager
	cache                    *MetaCache
}

func NewPgMetaDescriptionSyncer(globalTransactionManager *PgDbTransactionManager) *PgMetaDescriptionSyncer {
	md := PgMetaDescriptionSyncer{globalTransactionManager, NewCache()}
	md.CreateMetaTableIfNotExists()
	return &md
}

func (md *PgMetaDescriptionSyncer) Cache() *MetaCache {
	return md.cache
}

func (md *PgMetaDescriptionSyncer) Get(name string) (*description.MetaDescription, bool, error) {
	globalTransaction, err := md.globalTransactionManager.BeginTransaction()
	if err != nil {
		return nil, false, err
	}
	tx := globalTransaction.Transaction().(*sql.Tx)
	if exists, err := checkMetaObjExists(name, tx); err != nil {
		logger.Debug("Error while checking if object exists %s: %s", name, err.Error())
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return nil, false, errors.NewFatalError(
			"get_meta",
			fmt.Sprintf("Error while checking if object exists %s: %s", name, err.Error()),
			nil,
		)
	} else if !exists {
		logger.Debug("Meta object %s does not exists.", name)
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return nil, false, errors.NewFatalError(
			"get_meta",
			fmt.Sprintf("Meta object %s does not exists .", name),
			nil,
		)
	}
	meta, err := getMetaObjFromDb(name, tx)
	if err != nil {
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		logger.Error("Can't get MetaDescription from DB '%s': %s", name, err.Error())
		return nil, false, nil
	}
	md.globalTransactionManager.CommitTransaction(globalTransaction)

	return &meta, true, nil

}

func (md *PgMetaDescriptionSyncer) List() ([]*description.MetaDescription, bool, error) {
	globalTransaction, err := md.globalTransactionManager.BeginTransaction()
	if err != nil {
		return nil, false, err
	}
	tx := globalTransaction.Transaction().(*sql.Tx)
	rows, err := tx.Query(SQL_GET_META_LIST)
	if err != nil {
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return nil, false, err
	}

	var metaList []*description.MetaDescription

	if err := rows.Err(); err != nil {
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return nil, false, errors.NewFatalError(
			"get_meta_list",
			fmt.Sprintf("Error during metaList retrive"),
			nil,
		)
	}
	for rows.Next() {
		var content string
		var meta = description.MetaDescription{}
		rows.Scan(&content)
		json.NewDecoder(strings.NewReader(content)).Decode(&meta)
		metaList = append(metaList, &meta)
	}
	md.globalTransactionManager.CommitTransaction(globalTransaction)

	return metaList, true, nil
}

func (md *PgMetaDescriptionSyncer) Remove(name string) (bool, error) {
	globalTransaction, err := md.globalTransactionManager.BeginTransaction()
	if err != nil {
		return false, err
	}
	tx := globalTransaction.Transaction().(*sql.Tx)
	if exists, err := checkMetaObjExists(name, tx); err != nil {
		logger.Debug("Error while checking if object exists %s: %s", name, err.Error())
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return false, errors.NewFatalError(
			"get_meta",
			fmt.Sprintf("Error while checking if object exists %s: %s", name, err.Error()),
			nil,
		)
	} else if !exists {
		logger.Debug("Meta object %s does not exists.", name)
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return false, errors.NewFatalError(
			"get_meta",
			fmt.Sprintf("Meta object %s does not exists.", name),
			nil,
		)
	}
	_, err = tx.Exec(DELETE_META_OBJ, name)
	if err != nil {
		logger.Error("Can't delete MetaDescription '%s': %s", name, err.Error())
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return true, errors.NewFatalError(
			"delete_meta_obj",
			fmt.Sprintf("Can't delete MetaDescription '%s'", name),
			nil,
		)
	}
	md.globalTransactionManager.CommitTransaction(globalTransaction)

	return true, nil
}

func (md *PgMetaDescriptionSyncer) Create(m description.MetaDescription) error {
	globalTransaction, err := md.globalTransactionManager.BeginTransaction()
	if err != nil {
		return err
	}
	tx := globalTransaction.Transaction().(*sql.Tx)
	if exists, err := checkMetaObjExists(m.Name, tx); err != nil {
		logger.Debug("Error while checking if object exists %s: %s", m.Name, err.Error())
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return errors.NewFatalError(
			"get_meta",
			fmt.Sprintf("Error while checking if object exists %s: %s", m.Name, err.Error()),
			nil,
		)

	} else if exists {
		logger.Debug("MetaDescription '%s' already exists", m.Name)
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return errors.NewFatalError(
			"create_meta_obj",
			fmt.Sprintf("MetaDescription '%s' already exists", m.Name),
			nil,
		)
	} else if exists == false {

		b, err := json.Marshal(m)
		if err != nil {
			md.globalTransactionManager.RollbackTransaction(globalTransaction)
			return errors.NewFatalError(
				"create_meta_obj",
				fmt.Sprintf("Can't json.Marshal '%s' MetaDescription", m.Name),
				nil,
			)
		}
		if _, err = tx.Exec(CREATE_META_OBJ, string(b)); err != nil {
			md.globalTransactionManager.RollbackTransaction(globalTransaction)
			logger.Error("Can't create MetaDescription '%s' : %s", m.Name, err.Error())
			return errors.NewFatalError(
				"create_meta_obj",
				fmt.Sprintf("Can't save MetaDescription'%s'", m.Name),
				nil,
			)
		}

	}
	md.globalTransactionManager.CommitTransaction(globalTransaction)
	return nil
}

func (md *PgMetaDescriptionSyncer) Update(name string, m description.MetaDescription) (bool, error) {
	globalTransaction, err := md.globalTransactionManager.BeginTransaction()
	if err != nil {
		return false, err
	}
	tx := globalTransaction.Transaction().(*sql.Tx)
	if exists, err := checkMetaObjExists(name, tx); err != nil {
		logger.Debug("Error while checking if object exists %s: %s", name, err.Error())
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return false, errors.NewFatalError(
			"get_meta",
			fmt.Sprintf("Error while checking if object exists %s: %s", name, err.Error()),
			nil,
		)
	} else if !exists {
		logger.Debug("Meta object %s does not exists.", name)
		md.globalTransactionManager.RollbackTransaction(globalTransaction)
		return false, errors.NewFatalError(
			"get_meta",
			fmt.Sprintf("Meta object %s does not exists.", name),
			nil,
		)
	} else if exists {

		b, err := json.Marshal(m)
		if err != nil {
			md.globalTransactionManager.RollbackTransaction(globalTransaction)
			return false, errors.NewFatalError(
				"update_meta_obj",
				fmt.Sprintf("Can't json.Marshal '%s' MetaDescription during update", name),
				nil,
			)
		}
		if _, err = tx.Exec(UPDATE_META_OBJ, string(b), name); err != nil {
			md.globalTransactionManager.RollbackTransaction(globalTransaction)
			logger.Error("Can't update MetaDescription '%s' : %s", name, err.Error())
			return false, errors.NewFatalError(
				"update_meta_obj",
				fmt.Sprintf("Can't update MetaDescription'%s'", name),
				nil,
			)
		}
	}
	md.globalTransactionManager.CommitTransaction(globalTransaction)
	return true, nil
}
