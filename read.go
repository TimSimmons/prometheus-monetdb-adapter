package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	_ "github.com/fajran/go-monetdb"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

var upLabels = []string{"job", "instance"}

func initRead(db *sql.DB) {
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		log.Println("handling read")
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

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// TODO: Support reading from more than one reader and merging the results.
		//if len(readers) != 1 {
		//	http.Error(w, fmt.Sprintf("expected exactly one reader, found %d readers", len(readers)), http.StatusInternalServerError)
		//	return
		//}
		//reader := readers[0]

		var resp *prompb.ReadResponse
		resp, err = handleRead(db, &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func handleRead(db *sql.DB, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	promTimeseries := []*prompb.TimeSeries{}

	for _, q := range req.Queries {
		log.Printf("%+v", q)

		// TODO: also return metric name?
		query, err := buildQuery(q)
		metricName := "up"
		if err != nil {
			return nil, err
		}
		log.Println(query)

		rows, err := db.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		// look up labels for metric name
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		log.Printf("columns: %s", cols)
		// TODO: delete ^

		rawTimeseries := make(map[string][]*prompb.Sample)
		rowCount := 0
		for rows.Next() {
			rowCount++

			// gymnastics to scan row into pointers
			timestamp := new(int)
			value := new(float64)
			rowScan := []interface{}{timestamp, value}
			for _ = range upLabels {
				rowScan = append(rowScan, new(string))
			}

			// read the row in
			err := rows.Scan(rowScan...)
			if err != nil {
				log.Printf("error scanning rows: %s", err)
				return nil, err
			}

			// get labels back out as strings
			rawLabels := rowScan[2:]
			labels := make([]string, len(upLabels))
			for i := range labels {
				v, ok := rawLabels[i].(*string)
				if !ok {
					return nil, fmt.Errorf("could coerce interface for column value %+v to a string", v)
				}
				labels[i] = *v
			}

			tsLabelKey := strings.Join(labels, ",")
			// log.Printf("processing row with labelKey %s: timestamp: %d, value %f", tsLabelKey, *timestamp, *value) // delete me

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

		err = rows.Err()
		if err != nil {
			return nil, err
		}

		// for each timeseries we found, make a Prometheus timeseries and attach the samples
		for labels, samples := range rawTimeseries {
			// create a label for the __name__ label
			labelPairs := []*prompb.Label{
				&prompb.Label{
					Name:  model.MetricNameLabel,
					Value: metricName,
				},
			}

			// split the ordered label values and match them up with the labels
			splitLabelValues := strings.Split(labels, ",")
			for i := range upLabels {
				labelPairs = append(labelPairs, &prompb.Label{
					Name:  upLabels[i],
					Value: splitLabelValues[i],
				})
			}

			promTimeseries = append(promTimeseries, &prompb.TimeSeries{
				Labels:  labelPairs,
				Samples: samples,
			})

			log.Printf("returning timeseries with label values %s with %d samples", splitLabelValues, len(samples))

		}
	}

	return &prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{
				Timeseries: promTimeseries,
			},
		},
	}, nil
}

func buildQuery(q *prompb.Query) (string, error) {
	matchers := make([]string, 0, len(q.Matchers))

	from := ""
	for _, m := range q.Matchers {
		if m.Name == model.MetricNameLabel {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				// TODO: check if name is a valid table?
				from = fmt.Sprintf("FROM %q", m.Value)
			default:
				// TODO: Figure out how to support these.
				return "", fmt.Errorf("non-equal or regex/regex-non-equal matchers are not supported on the metric name yet")
			}
			continue
		}

		if m.Name == "lts" {
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

	// TODO: Group by timeseries?
	// TODO: specify labels in select from some store of the columns
	return fmt.Sprintf("SELECT timestamp, value, job, instance %s WHERE %v;", from, strings.Join(matchers, " AND ")), nil
}

func escapeSingleQuotes(str string) string {
	return strings.Replace(str, `'`, `\'`, -1)
}

func replaceDotStar(str string) string {
	return strings.Replace(str, `.*`, `%`, -1)
}
