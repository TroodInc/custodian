package object

import (
	"custodian/logger"
	"database/sql"
	"log"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib" // needed for proper driver work
)

func getDBConnection(dbInfo string) *sql.DB {
	db, err := sql.Open("pgx", dbInfo)
	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(50)
	db.SetMaxOpenConns(50)
	if err != nil {
		logger.Error("%s", err)
		logger.Error("Could not connect to Postgres.")

		return &sql.DB{}
	}

	return db
}

var activeDBConnection *sql.DB

func NewDbConnection(dbInfo string) (*sql.DB, error) {
	if activeDBConnection == nil {
		activeDBConnection = getDBConnection(dbInfo)
	}
	alive := activeDBConnection.Ping()

	for alive != nil {
		log.Print("Connection to Postgres was lost. Waiting for 5s...")
		activeDBConnection.Close()
		time.Sleep(5 * time.Second)
		log.Print("Reconnecting...")
		activeDBConnection = getDBConnection(dbInfo)
		alive = activeDBConnection.Ping()
	}

	return activeDBConnection, nil
}
