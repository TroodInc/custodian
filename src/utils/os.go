package utils

import (
	"os"
	"logger"
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

func Intersection(a, b []string) []string {
	intersection := make([]string, 0)
	for _, aItem := range a {
		for _, bItem := range b {
			if aItem == bItem {
				intersection = append(intersection, aItem)
			}
		}
	}
	return intersection
}
