package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	//_ "github.com/fajran/go-monetdb"
	_ "github.internal.digitalocean.com/observability/monet/driver"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

var tableCreateLock sync.Mutex

var labelsMap = map[string]string{}
var labelsMapLock sync.Mutex

var metricWhitelist = map[string]bool{}

// meta table
var metaTableName string = "prometheus_adapter_meta"

var createMetaTableQuery string = `
CREATE TABLE "prometheus_adapter_meta" ("metric" VARCHAR(120), "labels" VARCHAR(120));`

var insertMetaTableQuery string = `
INSERT INTO prometheus_adapter_meta VALUES ('%s', '%s');`

var selectMetaTableQuery string = `
SELECT labels FROM prometheus_adapter_meta WHERE metric = '%s';`

var selectAllMetaTableQuery string = `
SELECT metric, labels FROM prometheus_adapter_meta;`

// metric tables
var createTableQuery string = `
CREATE TABLE "%s" ("timestamp" BIGINT, "value" FLOAT%s);`

//var insertMetricQuery string = `
//INSERT INTO "%s" VALUES (?, ?%s);`
var insertMetricQuery string = `INSERT INTO "%s" VALUES (%s);`

// find all tables
var listTablesQuery string = `
SELECT name FROM sys.tables WHERE tables.system=false;`

func initDB(dbURL string, whitelist string) (*sql.DB, error) {
	// connect to database
	db, err := sql.Open("monetdb", dbURL)
	if err != nil {
		return nil, errors.Wrap(err, "open DB")
	}

	err = db.Ping()
	if err != nil {
		return nil, errors.Wrap(err, "ping DB")
	}
	log.Println("connected successfully")

	// create meta table if it doesn't exist
	exists, err := tableExists(db, metaTableName)
	if err != nil {
		return nil, errors.Wrap(err, "meta table existence")
	}
	if !exists {
		err = createMetaTable(db)
		if err != nil {
			return nil, errors.Wrap(err, "create meta table")
		}
	}

	// init labelsMap
	err = refreshLabelsMap(db)
	if err != nil {
		return nil, errors.Wrap(err, "refresh labels map")
	}

	// keep the labelsMap up to date
	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for _ = range ticker.C {
			refreshLabelsMap(db)
		}
	}()

	// init static Metric whitelist
	for _, name := range strings.Split(whitelist, ",") {
		metricWhitelist[name] = true
	}

	// db.SetConnMaxLifetime(30 * time.Second)
	// db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(1000)

	// keep the labelsMap up to date
	ticker2 := time.NewTicker(5 * time.Second)
	go func() {
		for _ = range ticker2.C {
			stats := db.Stats()
			openConns.Set(float64(stats.OpenConnections))
		}
	}()

	return db, nil
}

func tableExists(db *sql.DB, name string) (bool, error) {
	tableCreateLock.Lock()
	defer tableCreateLock.Unlock()

	var tableName string
	rows, err := db.Query(listTablesQuery)
	dbQueries.Inc()
	if err != nil {
		queryErrors.Inc()
		return false, errors.Wrap(err, "list tables query")
	}
	defer rows.Close()

	var rowCounter float64
	for rows.Next() {
		rowCounter++
		err := rows.Scan(&tableName)
		if err != nil {
			rowScanErrors.Inc()
			return false, errors.Wrap(err, "scan list tables rows")
		}
		if tableName == name {
			return true, nil
		}
	}

	rowsRead.Add(rowCounter)
	err = rows.Err()
	if err != nil {
		rowErrors.Inc()
		return false, errors.Wrap(err, "list tables rows")
	}

	return false, nil
}

func createMetaTable(db *sql.DB) error {
	tableCreateLock.Lock()
	defer tableCreateLock.Unlock()

	_, err := db.Exec(createMetaTableQuery)
	dbQueries.Inc()
	if err != nil {
		queryErrors.Inc()
		return errors.Wrap(err, "create meta table")
	}

	tablesCreated.Inc()
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
	dbQueries.Inc()
	if err != nil {
		queryErrors.Inc()
		return errors.Wrap(err, "create metric table")
	}
	tablesCreated.Inc()
	log.Printf("created table %s", name)

	// create meta table entry
	query = fmt.Sprintf(insertMetaTableQuery, name, strings.Join(labels, ","))
	_, err = db.Exec(query)
	dbQueries.Inc()
	if err != nil {
		queryErrors.Inc()
		return errors.Wrap(err, "create meta table entry")
	}
	rowsInserted.Inc()
	log.Printf("inserted metatable entry %s", name)

	// new metric tabel means we need to refresh the labels cache
	err = refreshLabelsMap(db)
	if err != nil {
		return errors.Wrap(err, "refresh labels map")
	}

	return nil
}

// labelsMap functions

func refreshLabelsMap(db *sql.DB) error {
	labelsMapLock.Lock()
	defer labelsMapLock.Unlock()

	var (
		metric string
		labels string
	)

	newMap := map[string]string{}

	rows, err := db.Query(selectAllMetaTableQuery)
	dbQueries.Inc()
	if err != nil {
		queryErrors.Inc()
		return errors.Wrap(err, "select all meta table query")
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&metric, &labels)
		if err != nil {
			rowScanErrors.Inc()
			return errors.Wrap(err, "scan meta table rows")
		}
		newMap[metric] = labels
		rowsRead.Inc()
	}

	err = rows.Err()
	if err != nil {
		rowErrors.Inc()
		return errors.Wrap(err, "meta table row")
	}

	labelsMap = newMap
	return nil
}

func getLabelsOrCreate(db *sql.DB, name string, metric model.Metric) ([]string, error) {
	labelStr, exists := labelsMap[name]
	if !exists {
		labels := make([]string, 0, len(metric)-1)
		for k := range metric {
			if string(k) != model.MetricNameLabel {
				labels = append(labels, string(k))
			}
		}
		err := createMetricTable(db, name, labels)
		if err != nil {
			return nil, err
		}
		labelStr, _ = labelsMap[name]
	}

	return strings.Split(labelStr, ","), nil
}

func getLabels(db *sql.DB, name string) ([]string, error) {
	labelStr, exists := labelsMap[name]
	if !exists {
		return nil, fmt.Errorf("could not find table for metric %s", name)
	}
	return strings.Split(labelStr, ","), nil
}
