package utils

import (
	"github.com/joho/godotenv"
	"os"
	"strings"
)

type AppConfig struct {
	UrlPrefix           string
	DbConnectionOptions string
}

func getRealWorkingDirectory() string {

	reverse := func(pathParts []string) []string {

		for i, j := 0, len(pathParts)-1; i < j; i, j = i+1, j-1 {
			pathParts[i], pathParts[j] = pathParts[j], pathParts[i]
		}
		return pathParts
	}

	potentialWorkingDirectory, _ := os.Getwd()
	reversedPathParts := reverse(strings.Split(potentialWorkingDirectory, "/"))

	realWorkingDirectoryPathParts := make([]string, 0)
	shouldAppend := false
	for _, pathPart := range reversedPathParts {
		if pathPart == "custodian" {
			shouldAppend = true
		}
		if shouldAppend && len(pathPart) > 0 {
			realWorkingDirectoryPathParts = append(realWorkingDirectoryPathParts, pathPart)
		}
	}
	return "/" + strings.Join(reverse(realWorkingDirectoryPathParts), "/")

}

func GetConfig() *AppConfig {

	godotenv.Load(getRealWorkingDirectory() + "/.env")

	var appConfig = AppConfig{"/custodian", "host=localhost port=5432 dbname=custodian sslmode=disable"}

	if urlPrefix := os.Getenv("URL_PREFIX"); len(urlPrefix) > 0 {
		appConfig.UrlPrefix = urlPrefix
	}

	if dbConnectionOptions := os.Getenv("DB_CONNECTION_OPTIONS"); len(dbConnectionOptions) > 0 {
		appConfig.DbConnectionOptions = dbConnectionOptions
	}

	return &appConfig
}
