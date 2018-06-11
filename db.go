package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/fajran/go-monetdb"
)

var createTableQuery string = `
CREATE TABLE "%s" ("timestamp" BIGINT, %s "value" FLOAT);`

var listTablesQuery string = `
SELECT name FROM sys.tables WHERE tables.system=false;`

var insertUpQuery string = `
INSERT INTO "up" VALUES (?, ?, ?, ?);`

func initDB() *sql.DB {
	db, err := sql.Open("monetdb", "monetdb:monetdb@monetdb:50000/db")
	if err != nil {
		log.Fatal(err)
	}
	// defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected successfully")

	if !tableExists(db, "up") {
		createTable(db, "up", []string{"instance", "job"})
	}

	return db
}

func tableExists(db *sql.DB, name string) bool {
	var tableName string
	rows, err := db.Query(listTablesQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&tableName)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("found table %s", tableName)
		if tableName == name {
			return true
		}
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	return false
}

func createTable(db *sql.DB, name string, labels []string) error {
	var fields strings.Builder

	for _, label := range labels {
		fields.WriteString(fmt.Sprintf("\"%s\" VARCHAR(120),", label))
	}

	query := fmt.Sprintf(createTableQuery, name, fields.String())
	_, err := db.Exec(query)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("created table %s", name)
	return nil
}
