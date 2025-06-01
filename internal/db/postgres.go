package db

import (
	"database/sql"
	"fmt"
	"log"
	"pgcr-dataset-processor/internal/config"
)

func Connect(datasource config.Datasource) (*sql.DB, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		datasource.Host, datasource.Port, datasource.User, datasource.Password, datasource.Database)
	log.Print("Connecting to postgres db...")
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("Error opening connection to database: %v", err)
	}
	return db, nil
}
