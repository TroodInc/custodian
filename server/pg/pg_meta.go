package pg

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
	SQL_GET_META_LIST     string = `SELECT meta_description from o___meta__;`
	GET_META_OBJ          string = `SELECT meta_description from o___meta__ where meta_description ->> 'name'=$1;`
	DELETE_META_OBJ       string = `DELETE FROM o___meta__ where meta_description ->> 'name'=$1;`
	CHECK_META_OBJ_EXISTS string = `SELECT 1 FROM o___meta__ WHERE meta_description ->> 'name'=$1;`
	CREATE_META_OBJ       string = `INSERT INTO o___meta__ (meta_description) VALUES ($1);`
	UPDATE_META_OBJ       string = `UPDATE o___meta__ SET meta_description=$1 WHERE meta_description ->> 'name'=$2;`
)

func checkMetaObjExists(name string, tx *sql.Tx) bool {
	var exists bool
	tx.QueryRow(CHECK_META_OBJ_EXISTS, name).Scan(&exists)
	return exists
}

func getMetaObjFromDb(name string, tx *sql.Tx) *json.Decoder {
	var metaObj string
	tx.QueryRow(GET_META_OBJ, name).Scan(&metaObj)
	js := json.NewDecoder(strings.NewReader(metaObj))
	return js
}

func (md *PgMetaDescriptionSyncer) CreateMetaTableIfNotExists() {
	dbTransaction, _ := md.transactionManager.BeginTransaction()
	tx := dbTransaction.Transaction().(*sql.Tx)
	tx.Exec(SQL_CREATE_META_TABLE)
	md.transactionManager.CommitTransaction(dbTransaction)
}

type PgMetaDescriptionSyncer struct {
	transactionManager *PgDbTransactionManager
}

func NewPgMetaDescriptionSyncer(transactionManager *PgDbTransactionManager) *PgMetaDescriptionSyncer {
	md := PgMetaDescriptionSyncer{transactionManager}
	md.CreateMetaTableIfNotExists()
	return &md
}

func (md *PgMetaDescriptionSyncer) Get(name string) (*description.MetaDescription, bool, error) {
	dbTransaction, _ := md.transactionManager.BeginTransaction()
	tx := dbTransaction.Transaction().(*sql.Tx)
	if exists := checkMetaObjExists(name, tx); exists == false {
		logger.Debug("MetaDescription '%s' does not exist", name)
		md.transactionManager.RollbackTransaction(dbTransaction)
		return nil, false, errors.NewFatalError(
			"get_meta",
			fmt.Sprintf("The MetaDescription '%s' not found", name),
			nil,
		)
	}
	var meta = description.MetaDescription{}
	if err := getMetaObjFromDb(name, tx).Decode(&meta); err != nil {
		md.transactionManager.RollbackTransaction(dbTransaction)
		logger.Error("Can't parse MetaDescription '%s': %s", name, err.Error())
		return md.Get(name)
	}
	md.transactionManager.CommitTransaction(dbTransaction)

	return &meta, true, nil

}

func (md *PgMetaDescriptionSyncer) List() ([]*description.MetaDescription, bool, error) {
	dbTransaction, _ := md.transactionManager.BeginTransaction()
	tx := dbTransaction.Transaction().(*sql.Tx)
	rows, _ := tx.Query(SQL_GET_META_LIST)
	var metaList []*description.MetaDescription

	if err := rows.Err(); err != nil {
		md.transactionManager.RollbackTransaction(dbTransaction)
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
	md.transactionManager.CommitTransaction(dbTransaction)

	return metaList, true, nil
}

func (md *PgMetaDescriptionSyncer) Remove(name string) (bool, error) {
	dbTransaction, _ := md.transactionManager.BeginTransaction()
	tx := dbTransaction.Transaction().(*sql.Tx)
	if exists := checkMetaObjExists(name, tx); exists == false {
		md.transactionManager.RollbackTransaction(dbTransaction)
		logger.Debug("MetaDescription '%s' does not exist", name)
		return false, errors.NewFatalError(
			"delete_meta_obj",
			fmt.Sprintf("The MetaDescription '%s' not found", name),
			nil,
		)
	}
	_, err := tx.Exec(DELETE_META_OBJ, name)
	if err != nil {
		logger.Error("Can't delete MetaDescription '%s': %s", name, err.Error())
		md.transactionManager.RollbackTransaction(dbTransaction)
		return true, errors.NewFatalError(
			"delete_meta_obj",
			fmt.Sprintf("Can't delete MetaDescription '%s'", name),
			nil,
		)
	}
	md.transactionManager.CommitTransaction(dbTransaction)

	return true, nil
}

func (md *PgMetaDescriptionSyncer) Create(m description.MetaDescription) error {
	dbTransaction, _ := md.transactionManager.BeginTransaction()
	tx := dbTransaction.Transaction().(*sql.Tx)
	if exists := checkMetaObjExists(m.Name, tx); exists == true {
		logger.Debug("MetaDescription '%s' already exists", m.Name)
		md.transactionManager.RollbackTransaction(dbTransaction)
		return errors.NewFatalError(
			"create_meta_obj",
			fmt.Sprintf("MetaDescription '%s' already exists", m.Name),
			nil,
		)
	} else if exists == false {

		b, err := json.Marshal(m)
		if err != nil {
			md.transactionManager.RollbackTransaction(dbTransaction)
			return errors.NewFatalError(
				"create_meta_obj",
				fmt.Sprintf("Can't json.Marshal '%s' MetaDescription", m.Name),
				nil,
			)
		}
		if _, err = tx.Exec(CREATE_META_OBJ, string(b)); err != nil {
			md.transactionManager.RollbackTransaction(dbTransaction)
			logger.Error("Can't create MetaDescription '%s' : %s", m.Name, err.Error())
			return errors.NewFatalError(
				"create_meta_obj",
				fmt.Sprintf("Can't save MetaDescription'%s'", m.Name),
				nil,
			)
		}

	}
	md.transactionManager.CommitTransaction(dbTransaction)
	return nil
}

func (md *PgMetaDescriptionSyncer) Update(name string, m description.MetaDescription) (bool, error) {
	dbTransaction, _ := md.transactionManager.BeginTransaction()
	tx := dbTransaction.Transaction().(*sql.Tx)
	if exists := checkMetaObjExists(name, tx); exists == false {
		md.transactionManager.RollbackTransaction(dbTransaction)
		logger.Debug("MetaDescription '%s' does not exist", name)
		return false, errors.NewFatalError(
			"update_meta_obj",
			fmt.Sprintf("The MetaDescription '%s' not found", name),
			nil,
		)
	} else if exists == true {

		b, err := json.Marshal(m)
		if err != nil {
			md.transactionManager.RollbackTransaction(dbTransaction)
			return false, errors.NewFatalError(
				"update_meta_obj",
				fmt.Sprintf("Can't json.Marshal '%s' MetaDescription during update", m.Name),
				nil,
			)
		}
		if _, err = tx.Exec(UPDATE_META_OBJ, string(b), m.Name); err != nil {
			md.transactionManager.RollbackTransaction(dbTransaction)
			logger.Error("Can't update MetaDescription '%s' : %s", m.Name, err.Error())
			return false, errors.NewFatalError(
				"update_meta_obj",
				fmt.Sprintf("Can't update MetaDescription'%s'", m.Name),
				nil,
			)
		}
	}
	md.transactionManager.CommitTransaction(dbTransaction)
	return true, nil
}
