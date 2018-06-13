package main

import (
	"net/http"
)

//TODO: config
// TODO: eliminate log.Fatals
// TODO: figure out in mem/config'd timeseries selection so we're not querying meta on every insert
func main() {
	db := initDB()
	defer db.Close()

	initMetrics(":8080")
	initRead(db)
	initRcv(db)

	http.ListenAndServe(":1234", nil)
}
