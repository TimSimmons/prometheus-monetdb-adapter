package main

import (
	"net/http"
)

func main() {
	db := initDB()
	defer db.Close()

	initRead(db)
	initRcv(db)

	http.ListenAndServe(":1234", nil)
}
