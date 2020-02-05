package transactions

import (
	"bufio"
	"encoding/json"
	"fmt"
	"logger"
	"os"
	"path"
	"path/filepath"
	"server/errors"
	"utils"
)

type FileMetaDescriptionSyncer struct {
	dir string
}

func NewFileMetaDescriptionSyncer(d string) *FileMetaDescriptionSyncer {
	return &FileMetaDescriptionSyncer{d}
}

func createMetaFile(metaFile string, m interface{}) error {
	f, err := os.Create(metaFile)
	if err != nil {
		logger.Error("Can't create file '%s'", metaFile, err.Error())
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

func getMetaList(directory string, extension string) []string {
	fpath := path.Join(directory, fmt.Sprintf("*.%s", extension))
	files, _ := filepath.Glob(fpath)
	var metaList []string
	for _, filename := range (files) {
		metaList = append(metaList, filename[0:len(filename)-(len(extension)+1)])
	}
	return metaList
}

func (fm *FileMetaDescriptionSyncer) List() ([]map[string]interface{}, bool, error) {
	var metaList []map[string]interface{}
	for _, metaFileName := range getMetaList(fm.dir, "json") {
		meta, _, _ := fm.Get(metaFileName)
		metaList = append(metaList, meta)
	}
	return metaList, true, nil
}

func (fm *FileMetaDescriptionSyncer) Get(name string) (map[string]interface{}, bool, error) {
	var metaFile = fm.getMetaFileName(name)
	if _, err := os.Stat(metaFile); err != nil {
		logger.Debug("File '%s' of MetaDescription '%s' not found", metaFile, name)
		return nil, false, errors.NewFatalError(
			"meta_file_get",
			fmt.Sprintf("The MetaDescription '%s' not found", name),
			nil,
		)
	}
	f, err := os.Open(metaFile)
	if err != nil {
		logger.Error("Can't open file '%s' of  MetaDescription '%s': %s", metaFile, name, err.Error())
		return nil, true, errors.NewFatalError(
			"meta_file_get",
			fmt.Sprintf("Can't open file of MetaDescription '%s'", name),
			nil,
		)
	}
	defer utils.CloseFile(f)

	var metaObj map[string]interface{}
	if err := json.NewDecoder(bufio.NewReader(f)).Decode(&metaObj); err != nil {
		logger.Error("Can't parse file '%s' of  MetaDescription '%s': %s", metaFile, name, err.Error())
		return fm.Get(name)
		//return nil, true, NewMetaError(name, "meta_file_get", ErrInternal, "Can't parse MetaDescription '%s'", name)
	}

	return metaObj, true, nil
}

func (fm *FileMetaDescriptionSyncer) Create(transaction MetaDescriptionTransaction, name string, m map[string]interface{}) error {
	var metaFile = fm.getMetaFileName(name)
	if _, err := os.Stat(metaFile); err == nil {
		logger.Debug("File '%s' of MetaDescription '%s' already exists: %s", metaFile, name)
		return errors.NewFatalError(
			"meta_file_create",
			fmt.Sprintf("The MetaDescription '%s' already exists", name),
			nil,
		)
	}

	if err := createMetaFile(metaFile, &m); err != nil {
		logger.Error("Can't save MetaDescription '%s' fo file '%s': %s", name, metaFile, err.Error())
		return errors.NewFatalError(
			"meta_file_create",
			fmt.Sprintf("Can't save MetaDescription'%s'", name),
			nil,
		)
	}
	transaction.AddCreatedMetaName(name)
	return nil
}


func (fm *FileMetaDescriptionSyncer) Remove(name string) (bool, error) {
	var metaFile = fm.getMetaFileName(name)
	if _, err := os.Stat(metaFile); err != nil {
		return false, errors.NewFatalError(
			"meta_file_remove",
			fmt.Sprintf("The MetaDescription '%s' not found", name),
			nil,
		)
	}
	err := os.Remove(metaFile)
	if err != nil {
		logger.Error("Can't delete file '%s' of MetaDescription '%s': %s", metaFile, name, err.Error())
		return true, errors.NewFatalError(
			"meta_file_remove",
			fmt.Sprintf("Can't delete MetaDescription '%s'", name),
			nil,
		)
	}
	return true, nil
}

func (fm *FileMetaDescriptionSyncer) Update(name string, m map[string]interface{}) (bool, error) {
	var metaFile = fm.getMetaFileName(name)
	if _, err := os.Stat(metaFile); err != nil {
		logger.Debug("Can't find file '%s' of MetaDescription '%s'", metaFile, name)
		return false, errors.NewFatalError(
			"meta_file_update",
			fmt.Sprintf("The MetaDescription '%s' not found", name),
			nil,
		)
	}

	var metaFileTmp = metaFile + ".tmp"
	if err := createMetaFile(metaFileTmp, &m); err != nil {
		logger.Error("Can't save temp file '%s' of MetaDescription '%s': %s", metaFile, name, err.Error())
		return true, errors.NewFatalError(
			"meta_file_update",
			fmt.Sprintf("Can't save MetaDescription'%s'", name),
			nil,
		)
	}

	if err := os.Rename(metaFileTmp, metaFile); err != nil {
		logger.Error("Can't rename temp file from '%s' to '%s' of MetaDescription '%s': %s", metaFileTmp, metaFile, name, err.Error())
		return true, errors.NewFatalError(
			"meta_file_update",
			fmt.Sprintf("Can't save MetaDescription '%s'", name),
			nil,
		)
	}

	return true, nil
}

func (fm *FileMetaDescriptionSyncer) getMetaFileName(metaName string) string {
	return path.Join(fm.dir, metaName+".json")
}
