package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var rowsInserted prometheus.Counter = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "monetdb_adapter_rows_inserted_total",
		Help: "Number of rows inserted into MonetDB.",
	})

var rowsRead prometheus.Counter = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "monetdb_adapter_rows_read_total",
		Help: "Number of rows read from MonetDB.",
	})

var readInFlight prometheus.Gauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "monetdb_adapter_reads_inflight",
	Help: "Number of current HTTP read requests happening.",
})

var writeInFlight prometheus.Gauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "monetdb_adapter_writes_inflight",
	Help: "Number of current HTTP read requests happening.",
})

var requestsCounter *prometheus.CounterVec = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "monetdb_adapter_http_requests_total",
		Help: "HTTP requests counter.",
	},
	[]string{"code", "method"},
)

var requestDuration *prometheus.HistogramVec = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "monetdb_adapter_http_request_duration_seconds",
		Help:    "A histogram of latencies for requests.",
		Buckets: []float64{.01, .05, .1, .15, .25, .5, 1, 2.5, 5, 10, 20, 30, 45, 60, 90, 120, 180},
	},
	[]string{"handler", "method"},
)

var readResponseSize *prometheus.HistogramVec = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "monetdb_adapter_http_read_response_size_bytes",
		Help:    "A histogram of response sizes for read requests.",
		Buckets: []float64{200, 500, 900, 1500, 5000, 10000, 50000, 100000, 250000, 500000, 1000000, 5000000, 10000000, 20000000, 30000000, 40000000, 50000000},
	},
	[]string{},
)

var writeResponseSize *prometheus.HistogramVec = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "monetdb_adapter_http_write_response_size_bytes",
		Help:    "A histogram of response sizes for write requests.",
		Buckets: []float64{200, 500, 900, 1500, 5000, 10000, 50000, 100000},
	},
	[]string{},
)

func initMetrics(addr string) {
	prometheus.MustRegister(rowsInserted, rowsRead, readInFlight, writeInFlight, requestsCounter, requestDuration, readResponseSize, writeResponseSize)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("exposing prometheus metrics at %s", addr)

		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
}
