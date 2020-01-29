package v2_meta

import (
	"bufio"
	"encoding/json"
	"logger"
	"os"
	"path"
	"fmt"
	"path/filepath"
	"server/errors"
	"server/transactions"
	. "server/object/description"
	"utils"
)

type FileMetaDescriptionSyncer struct {
	dir string
}

func NewFileMetaDescriptionSyncer(d string) *FileMetaDescriptionSyncer {
	return &FileMetaDescriptionSyncer{d}
}

func createMetaFile(metaFile string, m *V2Meta) error {
	f, err := os.Create(metaFile)
	if err != nil {
		logger.Error("Can't create file '%s': %s", metaFile, m.Name, err.Error())
		return err
	}
	defer utils.CloseFile(f)

	w := bufio.NewWriter(f)
	if err := json.NewEncoder(w).Encode(m); err != nil {
		logger.Error("Can't encoding MetaDescription '%s' to file '%s': %s", m.Name, metaFile, err.Error())
		defer utils.RemoveFile(metaFile)
		return err
	}

	if err := w.Flush(); err != nil {
		logger.Error("Can't write MetaDescription '%s' to file '%s': %s", m.Name, metaFile, err.Error())
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

func (fm *FileMetaDescriptionSyncer) List() ([]*V2Meta, bool, error) {
	var metaList []*V2Meta
	for _, metaFileName := range getMetaList(fm.dir, "json") {
		meta, _, _ := fm.Get(metaFileName)
		metaList = append(metaList, meta)
	}
	return metaList, true, nil
}

func (fm *FileMetaDescriptionSyncer) Get(name string) (*V2Meta, bool, error) {
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

	var meta = V2Meta{}
	if err := json.NewDecoder(bufio.NewReader(f)).Decode(&meta); err != nil {
		logger.Error("Can't parse file '%s' of  MetaDescription '%s': %s", metaFile, name, err.Error())
		return fm.Get(name)
		//return nil, true, NewMetaError(name, "meta_file_get", ErrInternal, "Can't parse MetaDescription '%s'", name)
	}

	return &meta, true, nil
}

func (fm *FileMetaDescriptionSyncer) Create(transaction transactions.MetaDescriptionTransaction, m V2Meta) error {
	var metaFile = fm.getMetaFileName(m.Name)
	if _, err := os.Stat(metaFile); err == nil {
		logger.Debug("File '%s' of MetaDescription '%s' already exists: %s", metaFile, m.Name)
		return errors.NewFatalError(
			"meta_file_create",
			fmt.Sprintf("The MetaDescription '%s' already exists", m.Name),
			nil,
		)
	}

	if err := createMetaFile(metaFile, &m); err != nil {
		logger.Error("Can't save MetaDescription '%s' fo file '%s': %s", m.Name, metaFile, err.Error())
		return errors.NewFatalError(
			"meta_file_create",
			fmt.Sprintf("Can't save MetaDescription'%s'", m.Name),
			nil,
		)
	}
	transaction.AddCreatedMetaName(m.Name)
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

func (fm *FileMetaDescriptionSyncer) Update(name string, m V2Meta) (bool, error) {
	var metaFile = fm.getMetaFileName(name)
	if _, err := os.Stat(metaFile); err != nil {
		logger.Debug("Can't find file '%s' of MetaDescription '%s'", metaFile, name)
		return false, errors.NewFatalError(
			"meta_file_update",
			fmt.Sprintf("The MetaDescription '%s' not found", name),
			nil,
		)
	}

	if name != m.Name {
		return true, errors.NewFatalError(
			"meta_file_update",
			fmt.Sprintf("Name of MetaDescription '%s' is not equal to name parameter %s", m.Name, name),
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
