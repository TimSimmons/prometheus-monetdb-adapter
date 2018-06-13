package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strings"

	_ "github.com/fajran/go-monetdb"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func initRcv(db *sql.DB) {
	receiveHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		samples := protoToSamples(&req)
		err = writeSamples(db, samples)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	})

	writeChain := promhttp.InstrumentHandlerInFlight(writeInFlight,
		promhttp.InstrumentHandlerDuration(requestDuration.MustCurryWith(prometheus.Labels{"handler": "write"}),
			promhttp.InstrumentHandlerCounter(requestsCounter,
				promhttp.InstrumentHandlerResponseSize(writeResponseSize, receiveHandler),
			),
		),
	)

	http.Handle("/receive", writeChain)
}

// protoToSamples converts the proto objects to Prometheus objects
func protoToSamples(req *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		// prepare a labelMap with all the labels
		metric := make(model.Metric, len(ts.Labels))
		// insert the labels/values
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		// build the corpus of samples we'll be inserting
		// TODO: Do selection based on db/whitelist
		if name, hasName := metric["__name__"]; hasName && (name == "up" || name == "prometheus_tsdb_head_samples_appended_total" || name == "prometheus_engine_query_duration_seconds") {
			for _, s := range ts.Samples {
				// skip NaN values. TODO: use 0?
				if math.IsNaN(s.Value) {
					continue
				}

				samples = append(samples, &model.Sample{
					Metric:    metric,
					Value:     model.SampleValue(s.Value),
					Timestamp: model.Time(s.Timestamp),
				})
			}
		}
	}
	return samples
}

// TODO: Batch/txn these?
func writeSamples(db *sql.DB, samples model.Samples) error {
	for _, sample := range samples {
		// figure out metric name
		var name string
		metric := sample.Metric
		if metName, hasName := metric[model.MetricNameLabel]; !hasName {
			continue
		} else {
			name = string(metName)
		}

		// get labels from database or create a table if it doesn't exist
		labels := getLabelsOrCreate(db, name, metric)

		// prepare the query with the right number of parameters and prep the values for the insert
		var queryQs strings.Builder
		metricLabelValues := []string{}
		for _, label := range labels {
			queryQs.WriteString(", ?")
			metricLabelValues = append(metricLabelValues, string(metric[model.LabelName(label)]))
		}
		stmt, err := db.Prepare(fmt.Sprintf(insertMetricQuery, name, queryQs.String()))
		if err != nil {
			log.Printf("prepare insertMetric: %s", err)
			return err
		}

		// prepare the query exec []interface{}
		execValues := []interface{}{sample.Timestamp, sample.Value}
		for _, label := range metricLabelValues {
			execValues = append(execValues, label)
		}

		// execute the query
		res, err := stmt.Exec(execValues...)
		if err != nil {
			log.Printf("exec insertMetric: %s", err)
			return err
		}

		// count the rows we inserted
		inserted, err := res.RowsAffected()
		if err != nil {
			log.Printf("error reading number of rows affected: %s", err)
		}
		rowsInserted.Add(float64(inserted))
	}

	return nil
}
