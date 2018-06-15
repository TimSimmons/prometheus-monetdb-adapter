package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strings"
)

// TODO: Something real with env vars and files and stuff
type config struct {
	dbURL           string
	metricWhitelist string
}

// TODO: allow regexes, or at least startswiths
var defaultMetricWhitelist string = strings.Join(defaultMetrics, ",")

var defaultMetrics []string = []string{
	"up",
	"monetdb_adapter_http_read_response_size_bytes_bucket",
	"monetdb_adapter_http_read_response_size_bytes_sum",
	"monetdb_adapter_http_read_response_size_bytes_count",
	"monetdb_adapter_http_request_duration_seconds_bucket",
	"monetdb_adapter_http_request_duration_seconds_sum",
	"monetdb_adapter_http_request_duration_seconds_count",
	"monetdb_adapter_http_requests_total",
	"monetdb_adapter_http_write_response_size_bytes_bucket",
	"monetdb_adapter_http_write_response_size_bytes_sum",
	"monetdb_adapter_http_write_response_size_bytes_count",
	"monetdb_adapter_queries_total",
	"monetdb_adapter_query_errors_total",
	"monetdb_adapter_reads_inflight",
	"monetdb_adapter_row_errors_total",
	"monetdb_adapter_rows_inserted_total",
	"monetdb_adapter_rows_read_total",
	"monetdb_adapter_rowscan_errors_total",
	"monetdb_adapter_tables_created_total",
	"monetdb_adapter_writes_inflight",
	"monetdb_adapter_open_connections_total",
}

func main() {
	conf := config{}
	flag.StringVar(&conf.dbURL, "dbURL", "monetdb:monetdb@monetdb:50000/db", "url for the MonetDB connection")
	flag.StringVar(&conf.metricWhitelist, "whitelist", defaultMetricWhitelist, "comma-separated list of metric names to ingest by default, you can also insert lines into the db manually")
	flag.Parse()

	db, err := initDB(conf.dbURL, conf.metricWhitelist)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	initMetrics(":8080")
	initRead(db)
	initWrite(db)

	http.ListenAndServe(":1234", nil)
}
