package utils

import (
	"os"
	"custodian/logger"
	"path/filepath"
)

func CloseFile(f *os.File) error {
	if err := f.Close(); err != nil {
		logger.Warn("Can't close file '%s': %s", f.Name(), err.Error())
		return err
	}
	return nil
}

func RemoveFile(path string) error {
	if err := os.Remove(path); err != nil {
		logger.Warn("Can't remove file '%s': %s", path, err.Error())
		return err
	}
	return nil
}

func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}
