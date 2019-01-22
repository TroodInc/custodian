package storage

import (
	"server/migrations/description"
	"os"
	"bufio"
	"encoding/json"
	"path"
	"utils"
)

const fileExtension = "json"

type MigrationStorage struct {
	storagePath string
}

func (ms *MigrationStorage) Store(description *description.MigrationDescription) (string, error) {
	migrationFileName := ms.generateMigrationFileName(description)
	f, err := os.Create(migrationFileName)
	if err != nil {
		return "", err
	}
	defer utils.CloseFile(f)

	w := bufio.NewWriter(f)
	if encodedData, err := json.MarshalIndent(description, "", "\t"); err != nil {
		defer ms.Remove(migrationFileName)
		return "", err
	} else {
		w.Write(encodedData)
	}

	if err := w.Flush(); err != nil {
		defer ms.Remove(migrationFileName)
		return "", err
	}
	return migrationFileName, nil
}

func (ms *MigrationStorage) Remove(migrationFileName string) error {
	return utils.RemoveFile(migrationFileName)
}

func (ms *MigrationStorage) Flush() error {
	return utils.RemoveContents(ms.storagePath)
}

func (ms *MigrationStorage) generateMigrationFileName(migrationDescription *description.MigrationDescription) string {
	migrationFileName := migrationDescription.Id + "." + fileExtension
	return path.Join(ms.storagePath, migrationFileName)
}

func NewMigrationStorage(storagePath string) *MigrationStorage {
	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		os.Mkdir(storagePath, os.ModePerm)
	}
	return &MigrationStorage{storagePath: storagePath}
}
