package meta

import (
	"bufio"
	"encoding/json"
	"logger"
	"os"
	"path"
	"fmt"
	"path/filepath"
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

func createMetaFile(metaFile string, m *meta) error {
	f, err := os.Create(metaFile)
	if err != nil {
		logger.Error("Can't create file '%s': %s", metaFile, m.Name, err.Error())
		return err
	}
	defer closeFile(f)

	w := bufio.NewWriter(f)
	if err := json.NewEncoder(w).Encode(m); err != nil {
		logger.Error("Can't encoding meta '%s' to file '%s': %s", m.Name, metaFile, err.Error())
		defer removeFile(metaFile)
		return err
	}

	if err := w.Flush(); err != nil {
		logger.Error("Can't write meta '%s' to file '%s': %s", m.Name, metaFile, err.Error())
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

func (fm *FileMetaDriver) List() (*[] *meta, bool, error) {
	var metaList []*meta
	for _, metaFileName := range getMetaList(fm.dir, "json") {
		meta, _, _ := fm.Get(metaFileName)
		metaList = append(metaList, meta)
	}
	return &metaList, true, nil
}

func (fm *FileMetaDriver) Get(name string) (*meta, bool, error) {
	var metaFile = path.Join(fm.dir, name+".json")
	if _, err := os.Stat(metaFile); err != nil {
		logger.Debug("File '%s' of meta '%s' not found", metaFile, name)
		return nil, false, NewMetaError(name, "meta_file_get", ErrNotFound, "The meta '%s' not found", name)
	}
	f, err := os.Open(metaFile)
	if err != nil {
		logger.Error("Can't open file '%s' of  meta '%s': %s", metaFile, name, err.Error())
		return nil, true, NewMetaError(name, "meta_file_get", ErrInternal, "Can't open file of meta '%s'", name)
	}
	defer closeFile(f)

	var meta = meta{}
	if err := json.NewDecoder(bufio.NewReader(f)).Decode(&meta); err != nil {
		logger.Error("Can't parse file '%s' of  meta '%s': %s", metaFile, name, err.Error())
		return nil, true, NewMetaError(name, "meta_file_get", ErrInternal, "Can't parse meta '%s'", name)
	}

	return &meta, true, nil
}

func (fm *FileMetaDriver) Create(m meta) error {
	var metaFile = path.Join(fm.dir, m.Name+".json")
	if _, err := os.Stat(metaFile); err == nil {
		logger.Debug("File '%s' of meta '%s' already exists: %s", metaFile, m.Name, err.Error())
		return NewMetaError(m.Name, "meta_file_create", ErrDuplicated, "The meta '%s' already exists", m.Name)
	}

	if err := createMetaFile(metaFile, &m); err != nil {
		logger.Error("Can't save meta '%s' fo file '%s': %s", m.Name, metaFile, err.Error())
		return NewMetaError(m.Name, "meta_file_create", ErrInternal, "Can't save meta'%s'", m.Name)
	}

	return nil
}

func (fm *FileMetaDriver) Remove(name string) (bool, error) {
	var metaFile = path.Join(fm.dir, name+".json")
	if _, err := os.Stat(metaFile); err != nil {
		return false, NewMetaError(name, "meta_file_remove", ErrNotFound, "The meta '%s' not found", name)
	}
	err := os.Remove(metaFile)
	if err != nil {
		logger.Error("Can't delete file '%s' of meta '%s': %s", metaFile, name, err.Error())
		return true, NewMetaError(name, "meta_file_remove", ErrInternal, "Can't delete meta '%s'", name)
	}
	return true, nil
}

func (fm *FileMetaDriver) Update(name string, m meta) (bool, error) {
	var metaFile = path.Join(fm.dir, name+".json")
	if _, err := os.Stat(metaFile); err != nil {
		logger.Debug("Can't find file '%s' of meta '%s'", metaFile, name)
		return false, NewMetaError(name, "meta_file_update", ErrNotFound, "The meta '%s' not found", name)
	}

	if name != m.Name {
		return true, NewMetaError(name, "meta_file_update", ErrNotValid, "Name of meta '%s' is not equal to name parameter %s", m.Name, name)
	}

	var metaFileTmp = metaFile + ".tmp"
	if err := createMetaFile(metaFileTmp, &m); err != nil {
		logger.Error("Can't save temp file '%s' of meta '%s': %s", metaFile, name, err.Error())
		return true, NewMetaError(name, "meta_file_update", ErrInternal, "Can't save meta'%s'", name)
	}

	if err := os.Rename(metaFileTmp, metaFile); err != nil {
		logger.Error("Can't rename temp file from '%s' to '%s' of meta '%s': %s", metaFileTmp, metaFile, name, err.Error())
		return true, NewMetaError(name, "meta_file_update", ErrInternal, "Can't save meta '%s'", name)
	}

	return true, nil
}
