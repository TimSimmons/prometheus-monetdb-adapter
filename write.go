package main

import (
	"database/sql"
	"io/ioutil"
	"log"
	"net/http"

	_ "github.com/fajran/go-monetdb"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/prompb"
)

func initRcv(db *sql.DB) {
	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
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
		writeSamples(db, samples)
	})
}

func protoToSamples(req *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		if name, hasName := metric["__name__"]; hasName && name == "up" {
			log.Println(metric)

			for _, s := range ts.Samples {
				samples = append(samples, &model.Sample{
					Metric:    metric,
					Value:     model.SampleValue(s.Value),
					Timestamp: model.Time(s.Timestamp),
				})

				log.Printf("  %f %d\n", s.Value, s.Timestamp)
			}
		}
	}
	return samples
}

func writeSamples(db *sql.DB, samples model.Samples) {
	for _, sample := range samples {
		stmt, err := db.Prepare(insertUpQuery)
		if err != nil {
			log.Fatal(err)
		}
		instance, _ := sample.Metric["instance"]
		job, _ := sample.Metric["job"]

		res, err := stmt.Exec(sample.Timestamp, instance, job, sample.Value)
		if err != nil {
			log.Fatal(err)
		}
		lastId, err := res.LastInsertId()
		if err != nil {
			log.Fatal(err)
		}
		rowCnt, err := res.RowsAffected()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("ID = %d, affected = %d\n", lastId, rowCnt)
	}
}

//func protoToSamples(req *prompb.WriteRequest) model.Samples {
//	var samples model.Samples
//	for _, ts := range req.Timeseries {
//		metric := make(model.Metric, len(ts.Labels))
//		for _, l := range ts.Labels {
//			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
//		}
//
//		fmt.Println(metric)
//		if name, hasName := metric['__name__']; hasName {
//
//		}
//
//		for _, s := range ts.Samples {
//			samples = append(samples, &model.Sample{
//				Metric:    metric,
//				Value:     model.SampleValue(s.Value),
//				Timestamp: model.Time(s.Timestamp),
//			})
//
//			fmt.Printf("  %f %d\n", s.Value, s.Timestamp)
//		}
//
//	}
//	return samples
//}
