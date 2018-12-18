package migrations

import (
	"encoding/json"
	"io"
	"server/migrations/description"
)

type MigrationSerializer struct {
}

func (ms *MigrationSerializer) Serialize() {}
func (ms *MigrationSerializer) Deserialize(inputReader io.ReadCloser) (*description.MigrationDescription, error) {
	migrationsDescription := new(description.MigrationDescription)
	if e := json.NewDecoder(inputReader).Decode(migrationsDescription); e != nil {
		return nil, NewMigrationDeserializationError(e.Error())
	}

	return nil, nil
	// normalize description
	//return metaStore.NewMeta((&NormalizationService{}).Normalize(&metaObj))
}
