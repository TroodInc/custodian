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

func getMetaObjFromDb(name string, tx *sql.Tx) (description.MetaDescription, error) {
	var metaObj string
	var meta = description.MetaDescription{}

	if err := tx.QueryRow(GET_META_OBJ, name).Scan(&metaObj); err != nil {
		return meta, err
	}

	jd := json.NewDecoder(strings.NewReader(metaObj))
	if err := jd.Decode(&meta); err != nil {
		return meta, err
	}

	return meta, nil
}

type PgMetaDescriptionSyncer struct {
	globalTransactionManager *PgDbTransactionManager
	cache                    *MetaCache
}

func NewPgMetaDescriptionSyncer(globalTransactionManager *PgDbTransactionManager, mc *MetaCache, db *sql.DB) *PgMetaDescriptionSyncer {
	if len(mc.metaList) == 0 {
		md := PgMetaDescriptionSyncer{globalTransactionManager, mc}
		db.Exec(SQL_CREATE_META_TABLE)

		metaDescriptionList, _, _ := md.List()
		mc.Fill(metaDescriptionList)
		return &md
	}
	md := PgMetaDescriptionSyncer{globalTransactionManager, mc}
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
	tx := globalTransaction.Transaction()

	meta, err := getMetaObjFromDb(name, tx)
	if err != nil {
		if err == sql.ErrNoRows {

			globalTransaction.Rollback()
			return nil, false, errors.NewFatalError(
				"get_meta",
				fmt.Sprintf("Meta object %s does not exists .", name),
				nil,
			)
		}

		globalTransaction.Rollback()
		logger.Error("Can't get MetaDescription from DB '%s': %s", name, err.Error())
		return nil, false, nil
	}

	globalTransaction.Commit()

	return &meta, true, nil

}

func (md *PgMetaDescriptionSyncer) List() ([]*description.MetaDescription, bool, error) {
	globalTransaction, err := md.globalTransactionManager.BeginTransaction()
	if err != nil {
		return nil, false, err
	}
	tx := globalTransaction.Transaction()
	rows, err := tx.Query(SQL_GET_META_LIST)
	if err != nil {

		globalTransaction.Rollback()
		return nil, false, err
	}

	defer rows.Close()

	var metaList []*description.MetaDescription

	for rows.Next() {
		var content string
		var meta = description.MetaDescription{}
		rows.Scan(&content)
		json.NewDecoder(strings.NewReader(content)).Decode(&meta)
		metaList = append(metaList, &meta)
	}
	if err := rows.Err(); err != nil {

		globalTransaction.Rollback()
		return nil, false, errors.NewFatalError(
			"get_meta_list",
			"Error during metaList retrive",
			nil,
		)
	}

	globalTransaction.Commit()

	return metaList, true, nil
}

func (md *PgMetaDescriptionSyncer) Remove(name string) (bool, error) {
	globalTransaction, err := md.globalTransactionManager.BeginTransaction()
	if err != nil {
		return false, err
	}
	tx := globalTransaction.Transaction()

	_, err = tx.Exec(DELETE_META_OBJ, name)
	if err != nil {
		logger.Error("Can't delete MetaDescription '%s': %s", name, err.Error())

		globalTransaction.Rollback()
		return true, errors.NewFatalError(
			"delete_meta_obj",
			fmt.Sprintf("Can't delete MetaDescription '%s'", name),
			nil,
		)
	}

	globalTransaction.Commit()
	md.Cache().Delete(name)
	return true, nil
}

func (md *PgMetaDescriptionSyncer) Create(m description.MetaDescription) error {
	globalTransaction, err := md.globalTransactionManager.BeginTransaction()
	if err != nil {
		return err
	}
	tx := globalTransaction.Transaction()

	b, err := json.Marshal(m)
	if err != nil {

		globalTransaction.Rollback()
		return errors.NewFatalError(
			"create_meta_obj",
			fmt.Sprintf("Can't json.Marshal '%s' MetaDescription", m.Name),
			nil,
		)
	}
	if _, err = tx.Exec(CREATE_META_OBJ, string(b)); err != nil {

		globalTransaction.Rollback()
		logger.Error("Can't create MetaDescription '%s' : %s", m.Name, err.Error())
		return errors.NewFatalError(
			"create_meta_obj",
			fmt.Sprintf("Can't save MetaDescription'%s'", m.Name),
			nil,
		)
	}

	globalTransaction.Commit()
	md.Cache().FactoryMeta(&m)

	return nil
}

func (md *PgMetaDescriptionSyncer) Update(name string, m description.MetaDescription) (bool, error) {
	globalTransaction, err := md.globalTransactionManager.BeginTransaction()
	if err != nil {
		return false, err
	}
	tx := globalTransaction.Transaction()

	b, err := json.Marshal(m)
	if err != nil {

		globalTransaction.Rollback()
		return false, errors.NewFatalError(
			"update_meta_obj",
			fmt.Sprintf("Can't json.Marshal '%s' MetaDescription during update", name),
			nil,
		)
	}
	if _, err = tx.Exec(UPDATE_META_OBJ, string(b), name); err != nil {

		globalTransaction.Rollback()
		logger.Error("Can't update MetaDescription '%s' : %s", name, err.Error())
		return false, errors.NewFatalError(
			"update_meta_obj",
			fmt.Sprintf("Can't update MetaDescription'%s'", name),
			nil,
		)
	}

	globalTransaction.Commit()
	md.Cache().FactoryMeta(&m)
	return true, nil
}
