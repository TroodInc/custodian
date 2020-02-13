package driver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"logger"
	"os"
	"path"
	"path/filepath"
	"server/errors"
	"server/object/meta"
	"server/pg"
	pg_transactions "server/pg/transactions"
	"server/transactions"
	"utils"
)

type JsonDriver struct {
	Syncer                   meta.MetaDbSyncer
	globalTransactionManager *transactions.GlobalTransactionManager
	metaFilesPath			 string
}

func NewJsonDriver(dbConnectionUrl, metaFilesPath string) *JsonDriver {
	md := transactions.NewFileMetaDescriptionSyncer(metaFilesPath)
	mds, _ := pg.NewSyncer(dbConnectionUrl)
	dataManager, _ := mds.NewDataManager()
	fileMetaTransactionManager := transactions.NewFileMetaDescriptionTransactionManager(md)
	dbTransactionManager := pg_transactions.NewPgDbTransactionManager(dataManager)
	gtm := transactions.NewGlobalTransactionManager(fileMetaTransactionManager, dbTransactionManager)
	return &JsonDriver{
		Syncer: mds,
		globalTransactionManager: gtm,
		metaFilesPath: metaFilesPath,
	}
}



func (j *JsonDriver) Get(name string) *meta.Meta {
	objMap, _, _ := j.getMeta(j.getMetaFileName(name))
	return meta.NewMetaFromMap(objMap)
}

func (j *JsonDriver) List() ([]*meta.Meta, error) {
	result := make([]*meta.Meta, 0)
	for _, metaFileName := range j.getMetaList() {
		obj, _, _ := j.getMeta(metaFileName)
		result = append(result, meta.NewMetaFromMap(obj))
	}

	return result, nil
}

func (j *JsonDriver) Create(object *meta.Meta) error {
	transaction, _ := j.globalTransactionManager.BeginTransaction()

	if e := j.Syncer.CreateObj(transaction.DbTransaction, object); e == nil {
		if e := j.createMeta(transaction.MetaDescriptionTransaction, object.Name, object.ForExport()); e == nil {
			j.globalTransactionManager.CommitTransaction(transaction)
			return nil
		} else {
			var e2 = j.Syncer.RemoveObj(transaction.DbTransaction, object.Name, false)
			logger.Error("Error while compenstaion of object '%s' metadata creation: %v", object.Name, e2)
			j.globalTransactionManager.RollbackTransaction(transaction)
			return e
		}
	} else {
		j.globalTransactionManager.RollbackTransaction(transaction)
		return e
	}
}

func (j *JsonDriver) Update(object *meta.Meta) (bool, error) {
	var metaFile = j.getMetaFileName(object.Name)
	if _, err := os.Stat(metaFile); err != nil {
		return false, errors.NewFatalError(
			"meta_file_update", fmt.Sprintf("The MetaDescription '%s' not found", object.Name), nil,
		)
	}

	var metaFileTmp = metaFile + ".tmp"
	if err := createMetaFile(metaFileTmp, object.ForExport()); err != nil {
		return true, errors.NewFatalError(
			"meta_file_update", fmt.Sprintf("Can't save MetaDescription'%s'", object.Name), nil,
		)
	}

	if err := os.Rename(metaFileTmp, metaFile); err != nil {
		return true, errors.NewFatalError(
			"meta_file_update", fmt.Sprintf("Can't save MetaDescription '%s'", object.Name), nil,
		)
	}

	globalTransaction, _ := j.globalTransactionManager.BeginTransaction()
	if err := j.Syncer.UpdateObj(globalTransaction.DbTransaction, object, object); err == nil {
		j.globalTransactionManager.CommitTransaction(globalTransaction)
		return true, nil
	} else {
		//rollback to the previous version
		if err := j.Syncer.UpdateObjTo(globalTransaction.DbTransaction, object); err != nil {
			j.globalTransactionManager.RollbackTransaction(globalTransaction)
			return false, err
		}

		if _, err := j.Update(object); err != nil {
			j.globalTransactionManager.RollbackTransaction(globalTransaction)
			return false, err
		}

		j.globalTransactionManager.RollbackTransaction(globalTransaction)
		return false, err
	}
}

func (j *JsonDriver) Remove(name string) (bool, error) {
	//remove object from the database
	transaction, _ := j.globalTransactionManager.BeginTransaction()
	if e := j.Syncer.RemoveObj(transaction.DbTransaction, name, true); e == nil {
		//remove object`s description *.json file
		var metaFile = j.getMetaFileName(name)
		if _, err := os.Stat(metaFile); err != nil {
			return false, errors.NewFatalError(
				"meta_file_remove", fmt.Sprintf("The MetaDescription '%s' not found", name), nil,
			)
		}
		err := os.Remove(metaFile)
		if err != nil {
			return false, errors.NewFatalError(
				"meta_file_remove", fmt.Sprintf("Can't delete MetaDescription '%s'", name), nil,
			)
		}

		j.globalTransactionManager.CommitTransaction(transaction)

		return true, err
	} else {
		j.globalTransactionManager.RollbackTransaction(transaction)
		return false, e
	}
}



func (j *JsonDriver) getMeta(metaFile string) (map[string]interface{}, bool, error) {
	if _, err := os.Stat(metaFile); err != nil {
		return nil, false, errors.NewFatalError(
			"meta_file_get",  fmt.Sprintf("The MetaDescription '%s' not found", metaFile), nil,
		)
	}
	f, err := os.Open(metaFile)
	if err != nil {
		return nil, true, errors.NewFatalError(
			"meta_file_get", fmt.Sprintf("Can't open file of MetaDescription '%s'", metaFile), nil,
		)
	}
	defer utils.CloseFile(f)

	var metaObj map[string]interface{}
	if err := json.NewDecoder(bufio.NewReader(f)).Decode(&metaObj); err != nil {
		return nil, true, errors.NewFatalError(
			"", fmt.Sprintf("Can't parse MetaDescription '%s'", metaFile), nil,
		)
	}

	return metaObj, true, nil
}

func (j *JsonDriver) getMetaList() []string {
	fpath := path.Join(j.metaFilesPath, "*.json")
	files, _ := filepath.Glob(fpath)
	return files
}

func (j *JsonDriver) getMetaFileName(metaName string) string {
	return path.Join(j.metaFilesPath, metaName+".json")
}

func (j *JsonDriver) createMeta(transaction transactions.MetaDescriptionTransaction, name string, m map[string]interface{}) error {
	var metaFile = j.getMetaFileName(name)
	if _, err := os.Stat(metaFile); err == nil {
		logger.Debug("File '%s' of MetaDescription '%s' already exists", metaFile, name)
		return errors.NewFatalError(
			"meta_file_create", fmt.Sprintf("The MetaDescription '%s' already exists", name), nil,
		)
	}

	if err := createMetaFile(metaFile, &m); err != nil {
		logger.Error("Can't save MetaDescription '%s' fo file '%s': %s", name, metaFile, err.Error())
		return errors.NewFatalError(
			"meta_file_create", fmt.Sprintf("Can't save MetaDescription'%s'", name), nil,
		)
	}
	transaction.AddCreatedMetaName(name)
	return nil
}



func createMetaFile(metaFile string, m interface{}) error {
	f, err := os.Create(metaFile)
	if err != nil {
		logger.Error("Can't create file '%s'", metaFile)
		return err
	}
	defer utils.CloseFile(f)

	w := bufio.NewWriter(f)
	if err := json.NewEncoder(w).Encode(m); err != nil {
		logger.Error("Can't encoding MetaDescription '%s' to file '%s'", metaFile, err.Error())
		defer utils.RemoveFile(metaFile)
		return err
	}

	if err := w.Flush(); err != nil {
		logger.Error("Can't write MetaDescription '%s' to file '%s'", metaFile, err.Error())
		defer utils.RemoveFile(metaFile)
		return err
	}

	return nil
}