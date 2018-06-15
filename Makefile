build:
	GOOS=linux go build -o prometheus_monetdb_adapter *.go

pprof:
	go tool pprof -svg http://localhost:1234/debug/pprof/heap > heap.svg
