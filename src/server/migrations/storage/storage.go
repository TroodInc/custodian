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
	migrationFileName := ms.generateMigrationFileName(description.Id)
	f, err := os.Create(migrationFileName)
	if err != nil {
		return "", err
	}
	defer utils.CloseFile(f)

	w := bufio.NewWriter(f)
	if encodedData, err := json.MarshalIndent(description, "", "\t"); err != nil {
		defer ms.RemoveFile(migrationFileName)
		return "", err
	} else {
		w.Write(encodedData)
	}

	if err := w.Flush(); err != nil {
		defer ms.RemoveFile(migrationFileName)
		return "", err
	}
	return migrationFileName, nil
}

func (ms *MigrationStorage) Remove(description *description.MigrationDescription) error {
	migrationFileName := ms.generateMigrationFileName(description.Id)
	return ms.RemoveFile(migrationFileName)
}

func (ms *MigrationStorage) RemoveFile(migrationFileName string) error {
	return utils.RemoveFile(migrationFileName)
}

func (ms *MigrationStorage) Get(migrationId string) (*description.MigrationDescription, error) {
	migrationFileName := ms.generateMigrationFileName(migrationId)
	if _, err := os.Stat(migrationFileName); err != nil {
		return nil, err
	}
	f, err := os.Open(migrationFileName)
	if err != nil {
		return nil, err
	}
	defer utils.CloseFile(f)

	migrationDescription := &description.MigrationDescription{}
	if err := json.NewDecoder(bufio.NewReader(f)).Decode(migrationDescription); err != nil {
		return nil, err
	}

	return migrationDescription, nil
}

func (ms *MigrationStorage) Flush() error {
	return utils.RemoveContents(ms.storagePath)
}

func (ms *MigrationStorage) generateMigrationFileName(migrationId string) string {
	migrationFileName := migrationId + "." + fileExtension
	return path.Join(ms.storagePath, migrationFileName)
}

func NewMigrationStorage(storagePath string) *MigrationStorage {
	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		os.Mkdir(storagePath, os.ModePerm)
	}
	return &MigrationStorage{storagePath: storagePath}
}
