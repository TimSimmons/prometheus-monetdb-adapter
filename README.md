# monet

This was an experiment I did to figure out if writing/reading to monetdb
via Prometheus `remote_read/write` would allow fast high-cardinality queries.

It didn't :P

## notes

Here's some random monetdb notes for next time

```
sudo monetdb stop db
sudo monetdb destroy db

sudo monetdb create db
sudo monetdb release db

call sys.querylog_enable();
select * from sys.querylog_catalog;
call sys.querylog_disable();

CREATE USER "adapter" WITH PASSWORD 'adapter' NAME 'Adapter Boi' SCHEMA "sys";
CREATE SCHEMA "adapter" AUTHORIZATION "adapter";
ALTER USER "adapter" SET SCHEMA "adapter";
```

## links

https://www.monetdb.org/Documentation/UserGuide/Tutorial
https://godoc.org/github.com/fajran/go-monetdb
https://hub.docker.com/r/monetdb/monetdb-r-docker/
https://github.com/prometheus/prometheus/blob/master/documentation/examples/remote_storage/remote_storage_adapter
https://github.com/timescale/prometheus-postgresql-adapter/blob/master/postgresql/client.go
https://godoc.org/github.com/prometheus/common/model
http://go-database-sql.org
https://prometheus.io/docs/prometheus/latest/configuration/configuration/#<remote_write>
https://github.com/prometheus/prometheus/blob/master/prompb/types.proto
https://news.ycombinator.com/item?id=11896105
https://jvns.ca/blog/2017/09/24/profiling-go-with-pprof/
