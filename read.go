package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

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

func initRead(db *sql.DB) {
	readHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("HTTP Error %v on /read, cause: %s", http.StatusInternalServerError, err)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			log.Printf("HTTP Error %v on /read, cause: %s", http.StatusBadRequest, err)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			log.Printf("HTTP Error %v on /read, cause: %s", http.StatusBadRequest, err)
			return
		}

		var resp *prompb.ReadResponse
		resp, err = readRequest(db, &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("HTTP Error %v on /read, cause: %s", http.StatusInternalServerError, err)
			return
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("HTTP Error %v on /read, cause: %s", http.StatusInternalServerError, err)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("HTTP Error %v on /read, cause: %s", http.StatusInternalServerError, err)
			return
		}
	})

	readChain := promhttp.InstrumentHandlerInFlight(readInFlight,
		promhttp.InstrumentHandlerDuration(requestDuration.MustCurryWith(prometheus.Labels{"handler": "read"}),
			promhttp.InstrumentHandlerCounter(requestsCounter,
				promhttp.InstrumentHandlerResponseSize(readResponseSize, readHandler),
			),
		),
	)

	http.Handle("/read", readChain)
}

func readRequest(db *sql.DB, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	start := time.Now()
	promTimeseries := []*prompb.TimeSeries{}

	for _, q := range req.Queries {
		// figure out the metric name (and thus the table name)
		name, err := getQueryMetricName(q)
		if err != nil {
			return nil, err
		}

		// look up labels for metric name
		labels, err := getLabels(db, name)
		if err != nil {
			return nil, err
		}

		// build the query
		query, err := buildQuery(q, name, labels)
		if err != nil {
			return nil, errors.Wrap(err, "build read query")
		}

		// execute the query
		rows, err := db.Query(query)
		dbQueries.Inc()
		if err != nil {
			queryErrors.Inc()
			return nil, errors.Wrap(err, "exec read metrics query")
		}
		defer rows.Close()

		// process rows, bucketing samples by timeseries label values
		rawTimeseries := make(map[string][]*prompb.Sample)
		rowCount := 0
		for rows.Next() {
			rowCount++

			// gymnastics to scan row into pointers
			timestamp := new(int)
			value := new(float64)
			rowScan := []interface{}{timestamp, value}
			for _ = range labels {
				rowScan = append(rowScan, new(string))
			}

			// read the row in
			err := rows.Scan(rowScan...)
			if err != nil {
				rowScanErrors.Inc()
				return nil, errors.Wrap(err, "scan metric rows")
			}

			// get labels back out as strings
			rawLabels := rowScan[2:]
			labels := make([]string, len(labels))
			for i := range labels {
				v, ok := rawLabels[i].(*string)
				if !ok {
					return nil, fmt.Errorf("could coerce interface for column value %+v to a string", v)
				}
				labels[i] = *v
			}

			// TODO: Metric.Fingerprint() here? https://godoc.org/github.com/prometheus/common/model#Metric.Fingerprint
			tsLabelKey := strings.Join(labels, ",")

			sample := &prompb.Sample{
				Timestamp: int64(*timestamp),
				Value:     *value,
			}

			ts, exists := rawTimeseries[tsLabelKey]
			if !exists {
				rawTimeseries[tsLabelKey] = []*prompb.Sample{sample}
				ts = rawTimeseries[tsLabelKey]
			} else {
				rawTimeseries[tsLabelKey] = append(ts, sample)
			}
		}

		rowsRead.Add(float64(rowCount))

		err = rows.Err()
		if err != nil {
			rowErrors.Inc()
			return nil, errors.Wrap(err, "read metric rows")
		}

		// for each timeseries we found, make a Prometheus timeseries and attach the samples
		for foundLabels, samples := range rawTimeseries {
			// create a label for the __name__ label
			labelPairs := []*prompb.Label{
				&prompb.Label{
					Name:  model.MetricNameLabel,
					Value: name,
				},
			}

			// split the ordered label values and match them up with the foundLabels
			splitLabelValues := strings.Split(foundLabels, ",")
			for i := range labels {
				labelPairs = append(labelPairs, &prompb.Label{
					Name:  labels[i],
					Value: splitLabelValues[i],
				})
			}

			promTimeseries = append(promTimeseries, &prompb.TimeSeries{
				Labels:  labelPairs,
				Samples: samples,
			})
		}
	}

	elapsed := time.Since(start)
	log.Printf("read query took %s", elapsed)

	return &prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{
				Timeseries: promTimeseries,
			},
		},
	}, nil
}

func buildQuery(q *prompb.Query, name string, labels []string) (string, error) {
	matchers := make([]string, 0, len(q.Matchers))

	for _, m := range q.Matchers {
		if m.Name == model.MetricNameLabel || m.Name == "remote_read" {
			continue
		}

		switch m.Type {
		case prompb.LabelMatcher_EQ:
			matchers = append(matchers, fmt.Sprintf("%q = '%s'", m.Name, escapeSingleQuotes(m.Value)))
		case prompb.LabelMatcher_NEQ:
			matchers = append(matchers, fmt.Sprintf("%q != '%s'", m.Name, escapeSingleQuotes(m.Value)))
		case prompb.LabelMatcher_RE:
			matchers = append(matchers, fmt.Sprintf("%q ILIKE '%s'", m.Name, replaceDotStar(m.Value)))
		case prompb.LabelMatcher_NRE:
			matchers = append(matchers, fmt.Sprintf("%q NOT ILIKE '%s'", m.Name, replaceDotStar(m.Value)))
		default:
			return "", fmt.Errorf("unknown match type %v", m.Type)
		}
	}
	matchers = append(matchers, fmt.Sprintf("timestamp >= %v", q.StartTimestampMs))
	matchers = append(matchers, fmt.Sprintf("timestamp <= %v", q.EndTimestampMs))

	// TODO: Group by timeseries value?
	return fmt.Sprintf("SELECT timestamp, value, %s FROM %s WHERE %v;", strings.Join(labels, ", "), name, strings.Join(matchers, " AND ")), nil
}

func getQueryMetricName(q *prompb.Query) (string, error) {
	for _, m := range q.Matchers {
		if m.Name == model.MetricNameLabel {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				return m.Value, nil
			default:
				// TODO: Figure out how to support these.
				return "", fmt.Errorf("non-equal or regex/regex-non-equal matchers are not supported on the metric name yet")
			}
			continue
		}
	}
	return "", fmt.Errorf("could not find metric name in query")
}

func escapeSingleQuotes(str string) string {
	return strings.Replace(str, `'`, `\'`, -1)
}

func replaceDotStar(str string) string {
	return strings.Replace(str, `.*`, `%`, -1)
}
