package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strings"

	//_ "github.com/fajran/go-monetdb"
	_ "github.internal.digitalocean.com/observability/monet/driver"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func initWrite(db *sql.DB) {
	writeHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("HTTP Error %v on /write, cause: %s", http.StatusInternalServerError, err)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			log.Printf("HTTP Error %v on /write, cause: %s", http.StatusBadRequest, err)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			log.Printf("HTTP Error %v on /write, cause: %s", http.StatusBadRequest, err)
			return
		}

		samples := protoToSamples(&req)
		err = writeSamples(db, samples)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("HTTP Error %v on /write, cause: %s", http.StatusInternalServerError, err)
			return
		}
	})

	writeChain := promhttp.InstrumentHandlerInFlight(writeInFlight,
		promhttp.InstrumentHandlerDuration(requestDuration.MustCurryWith(prometheus.Labels{"handler": "write"}),
			promhttp.InstrumentHandlerCounter(requestsCounter,
				promhttp.InstrumentHandlerResponseSize(writeResponseSize, writeHandler),
			),
		),
	)

	http.Handle("/write", writeChain)
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
		name, hasName := metric[model.MetricNameLabel]
		_, inLabelsMap := labelsMap[string(name)]
		_, inWhitelist := metricWhitelist[string(name)]

		// TODO: remove HasPrefix bit
		if hasName && (inLabelsMap || inWhitelist || strings.HasPrefix(string(name), "monetdb")) {
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

func writeSamples(db *sql.DB, samples model.Samples) error {
	statements := []string{}
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
		labels, err := getLabelsOrCreate(db, name, metric)
		if err != nil {
			return err
		}

		// build the query
		var metricLabelValues strings.Builder
		metricLabelValues.WriteString(fmt.Sprintf("%d ,", sample.Timestamp))
		metricLabelValues.WriteString(fmt.Sprintf(" %f", sample.Value))
		for _, label := range labels {
			metricLabelValues.WriteString(fmt.Sprintf(", '%s'", string(metric[model.LabelName(label)])))
		}

		statements = append(statements, fmt.Sprintf(insertMetricQuery, name, metricLabelValues.String()))
	}

	if len(statements) > 0 {
		tx, err := db.Begin()
		if err != nil {
			return errors.Wrap(err, "begin transaction")
		}

		inserts := int64(0)
		for _, stmt := range statements {
			res, err := tx.Exec(stmt)
			if err != nil {
				tx.Rollback()
				return errors.Wrap(err, "exec insert in transaction")
			}

			inserted, err := res.RowsAffected()
			if err != nil {
				// TODO: delete me
				log.Printf("error reading number of rows affected: %s", err)
			}

			inserts += inserted
		}

		err = tx.Commit()
		if err != nil {
			queryErrors.Inc()
			return errors.Wrap(err, "commit transacton")
		}
		dbQueries.Inc()
		rowsInserted.Add(float64(inserts))
	}

	return nil
}
