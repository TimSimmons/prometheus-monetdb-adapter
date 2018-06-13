package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"

	_ "github.com/fajran/go-monetdb"
	"github.com/prometheus/common/model"
)

var tableCreateLock sync.Mutex

// meta table
var metaTableName string = "prometheus_adapter_meta"

var createMetaTableQuery string = `
CREATE TABLE "prometheus_adapter_meta" ("metric" VARCHAR(120), "labels" VARCHAR(120));`

var insertMetaTableQuery string = `
INSERT INTO prometheus_adapter_meta VALUES ('%s', '%s');`

var selectMetaTableQuery string = `
SELECT labels FROM prometheus_adapter_meta WHERE metric = '%s';`

// metric tables
var createTableQuery string = `
CREATE TABLE "%s" ("timestamp" BIGINT, "value" FLOAT%s);`

var insertMetricQuery string = `
INSERT INTO "%s" VALUES (?, ?%s);`

// find all tables
var listTablesQuery string = `
SELECT name FROM sys.tables WHERE tables.system=false;`

func initDB() *sql.DB {
	db, err := sql.Open("monetdb", "monetdb:monetdb@monetdb:50000/db")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected successfully")

	if !tableExists(db, metaTableName) {
		err = createMetaTable(db)
		if err != nil {
			log.Fatal(err)
		}
	}

	return db
}

func tableExists(db *sql.DB, name string) bool {
	tableCreateLock.Lock()
	defer tableCreateLock.Unlock()

	var tableName string
	rows, err := db.Query(listTablesQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var rowCounter float64
	for rows.Next() {
		rowCounter++
		err := rows.Scan(&tableName)
		if err != nil {
			log.Fatal(err)
		}
		if tableName == name {
			return true
		}
	}

	rowsRead.Add(rowCounter)
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	return false
}

func createMetaTable(db *sql.DB) error {
	tableCreateLock.Lock()
	defer tableCreateLock.Unlock()

	_, err := db.Exec(createMetaTableQuery)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("created table %s", metaTableName)
	return nil
}

func createMetricTable(db *sql.DB, name string, labels []string) error {
	tableCreateLock.Lock()
	defer tableCreateLock.Unlock()

	var fields strings.Builder

	for _, label := range labels {
		fields.WriteString(fmt.Sprintf(",\"%s\" VARCHAR(120)", label))
	}

	// TODO Transaction this

	// create table
	query := fmt.Sprintf(createTableQuery, name, fields.String())
	_, err := db.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("created table %s", name)

	// create meta table entry
	query = fmt.Sprintf(insertMetaTableQuery, name, strings.Join(labels, ","))
	log.Printf("insert meta table entry query: %s", query)
	_, err = db.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("inserted metatable entry %s", name)

	return nil
}

// TODO: make this in memory
func getLabelsOrCreate(db *sql.DB, name string, metric model.Metric) []string {
	var labels []string
	if !tableExists(db, name) {
		labels = make([]string, 0, len(metric)-1)
		for k := range metric {
			if string(k) != model.MetricNameLabel {
				labels = append(labels, string(k))
			}
		}
		err := createMetricTable(db, name, labels)
		if err != nil {
			log.Fatalf("createMetricTable: %s", err)
		}
	} else {
		var tmpLabels string
		err := db.QueryRow(fmt.Sprintf(selectMetaTableQuery, name)).Scan(&tmpLabels)
		if err != nil {
			log.Fatalf("selectMetaTableQuery: %s", err)
		}
		labels = strings.Split(tmpLabels, ",")
	}
	return labels
}

func getLabels(db *sql.DB, name string) ([]string, error) {
	var labels []string
	if !tableExists(db, name) {
		return nil, fmt.Errorf("could not find table for metric %s", name)
	}

	var tmpLabels string
	err := db.QueryRow(fmt.Sprintf(selectMetaTableQuery, name)).Scan(&tmpLabels)
	if err != nil {
		log.Fatalf("selectMetaTableQuery: %s", err)
	}
	labels = strings.Split(tmpLabels, ",")

	return labels, nil
}
