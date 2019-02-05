package utils

import (
	"github.com/joho/godotenv"
	"os"
	"strings"
	"path"
)

type AppConfig struct {
	UrlPrefix               string
	DbConnectionOptions     string
	SentryDsn               string
	EnableProfiler          bool
	DisableSafePanicHandler bool
	MigrationStoragePath    string
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

//get Application configuration based on dotenv
func GetConfig() *AppConfig {

	godotenv.Load(getRealWorkingDirectory() + "/.env")

	var appConfig = AppConfig{
		UrlPrefix:               "/custodian",
		DbConnectionOptions:     "host=localhost port=5432 dbname=custodian sslmode=disable",
		SentryDsn:               "",
		EnableProfiler:          false,
		DisableSafePanicHandler: true,
		MigrationStoragePath:    path.Join(getRealWorkingDirectory(), "/applied_migrations"),
	}

	if urlPrefix := os.Getenv("URL_PREFIX"); len(urlPrefix) > 0 {
		appConfig.UrlPrefix = urlPrefix
	}

	if sentryDsn := os.Getenv("SENTRY_DSN"); len(sentryDsn) > 0 {
		appConfig.SentryDsn = sentryDsn
	}

	if dbConnectionOptions := os.Getenv("DB_CONNECTION_OPTIONS"); len(dbConnectionOptions) > 0 {
		appConfig.DbConnectionOptions = dbConnectionOptions
	}

	if enableProfiler := os.Getenv("ENABLE_PROFILER"); len(enableProfiler) > 0 {
		appConfig.EnableProfiler = enableProfiler == "true"
	}
	if disableSafePanicHandler := os.Getenv("DISABLE_SAFE_PANIC_HANDLER"); len(disableSafePanicHandler) > 0 {
		appConfig.DisableSafePanicHandler = disableSafePanicHandler == "true"
	}

	if migrationStoragePath := os.Getenv("MIGRATION_STORAGE_PATH"); len(migrationStoragePath) > 0 {
		if migrationStoragePath[0] == '/' {
			appConfig.MigrationStoragePath = migrationStoragePath
		} else {
			appConfig.MigrationStoragePath = path.Join(getRealWorkingDirectory(), migrationStoragePath)
		}
	}

	return &appConfig
}
