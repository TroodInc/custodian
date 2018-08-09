package meta

import (
	"bufio"
	"encoding/json"
	"logger"
	"os"
	"path"
	"fmt"
	"path/filepath"
	"server/transactions"
)

type FileMetaDriver struct {
	dir string
}

func NewFileMetaDriver(d string) *FileMetaDriver {
	return &FileMetaDriver{d}
}

func closeFile(f *os.File) error {
	if err := f.Close(); err != nil {
		logger.Warn("Can't close file '%s': %s", f.Name(), err.Error())
		return err
	}
	return nil
}

func removeFile(path string) error {
	if err := os.Remove(path); err != nil {
		logger.Warn("Can't remove file '%s': %s", path, err.Error())
		return err
	}
	return nil
}

func createMetaFile(metaFile string, m *MetaDescription) error {
	f, err := os.Create(metaFile)
	if err != nil {
		logger.Error("Can't create file '%s': %s", metaFile, m.Name, err.Error())
		return err
	}
	defer closeFile(f)

	w := bufio.NewWriter(f)
	if err := json.NewEncoder(w).Encode(m); err != nil {
		logger.Error("Can't encoding MetaDescription '%s' to file '%s': %s", m.Name, metaFile, err.Error())
		defer removeFile(metaFile)
		return err
	}

	if err := w.Flush(); err != nil {
		logger.Error("Can't write MetaDescription '%s' to file '%s': %s", m.Name, metaFile, err.Error())
		defer removeFile(metaFile)
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

func (fm *FileMetaDriver) List() (*[] *MetaDescription, bool, error) {
	var metaList []*MetaDescription
	for _, metaFileName := range getMetaList(fm.dir, "json") {
		meta, _, _ := fm.Get(metaFileName)
		metaList = append(metaList, meta)
	}
	return &metaList, true, nil
}

func (fm *FileMetaDriver) Get(name string) (*MetaDescription, bool, error) {
	var metaFile = fm.getMetaFileName(name)
	if _, err := os.Stat(metaFile); err != nil {
		logger.Debug("File '%s' of MetaDescription '%s' not found", metaFile, name)
		return nil, false, NewMetaError(name, "meta_file_get", ErrNotFound, "The MetaDescription '%s' not found", name)
	}
	f, err := os.Open(metaFile)
	if err != nil {
		logger.Error("Can't open file '%s' of  MetaDescription '%s': %s", metaFile, name, err.Error())
		return nil, true, NewMetaError(name, "meta_file_get", ErrInternal, "Can't open file of MetaDescription '%s'", name)
	}
	defer closeFile(f)

	var meta = MetaDescription{}
	if err := json.NewDecoder(bufio.NewReader(f)).Decode(&meta); err != nil {
		logger.Error("Can't parse file '%s' of  MetaDescription '%s': %s", metaFile, name, err.Error())
		return fm.Get(name)
		//return nil, true, NewMetaError(name, "meta_file_get", ErrInternal, "Can't parse MetaDescription '%s'", name)
	}

	return &meta, true, nil
}

func (fm *FileMetaDriver) Create(transaction transactions.MetaDescriptionTransaction, m MetaDescription) error {
	var metaFile = fm.getMetaFileName(m.Name)
	if _, err := os.Stat(metaFile); err == nil {
		logger.Debug("File '%s' of MetaDescription '%s' already exists: %s", metaFile, m.Name)
		return NewMetaError(m.Name, "meta_file_create", ErrDuplicated, "The MetaDescription '%s' already exists", m.Name)
	}

	if err := createMetaFile(metaFile, &m); err != nil {
		logger.Error("Can't save MetaDescription '%s' fo file '%s': %s", m.Name, metaFile, err.Error())
		return NewMetaError(m.Name, "meta_file_create", ErrInternal, "Can't save MetaDescription'%s'", m.Name)
	}
	transaction.AddCreatedMetaName(m.Name)
	return nil
}

func (fm *FileMetaDriver) Remove(name string) (bool, error) {
	var metaFile = fm.getMetaFileName(name)
	if _, err := os.Stat(metaFile); err != nil {
		return false, NewMetaError(name, "meta_file_remove", ErrNotFound, "The MetaDescription '%s' not found", name)
	}
	err := os.Remove(metaFile)
	if err != nil {
		logger.Error("Can't delete file '%s' of MetaDescription '%s': %s", metaFile, name, err.Error())
		return true, NewMetaError(name, "meta_file_remove", ErrInternal, "Can't delete MetaDescription '%s'", name)
	}
	return true, nil
}

func (fm *FileMetaDriver) Update(name string, m MetaDescription) (bool, error) {
	var metaFile = fm.getMetaFileName(name)
	if _, err := os.Stat(metaFile); err != nil {
		logger.Debug("Can't find file '%s' of MetaDescription '%s'", metaFile, name)
		return false, NewMetaError(name, "meta_file_update", ErrNotFound, "The MetaDescription '%s' not found", name)
	}

	if name != m.Name {
		return true, NewMetaError(name, "meta_file_update", ErrNotValid, "Name of MetaDescription '%s' is not equal to name parameter %s", m.Name, name)
	}

	var metaFileTmp = metaFile + ".tmp"
	if err := createMetaFile(metaFileTmp, &m); err != nil {
		logger.Error("Can't save temp file '%s' of MetaDescription '%s': %s", metaFile, name, err.Error())
		return true, NewMetaError(name, "meta_file_update", ErrInternal, "Can't save MetaDescription'%s'", name)
	}

	if err := os.Rename(metaFileTmp, metaFile); err != nil {
		logger.Error("Can't rename temp file from '%s' to '%s' of MetaDescription '%s': %s", metaFileTmp, metaFile, name, err.Error())
		return true, NewMetaError(name, "meta_file_update", ErrInternal, "Can't save MetaDescription '%s'", name)
	}

	return true, nil
}

func (fm *FileMetaDriver) getMetaFileName(metaName string) string {
	return path.Join(fm.dir, metaName+".json")
}
