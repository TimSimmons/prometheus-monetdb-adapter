FROM ubuntu:14.04

ADD prometheus_monetdb_adapter /opt/
ADD entrypoint.sh /opt/entrypoint.sh

ENV PORT 1234
EXPOSE 1234
WORKDIR /opt

ENTRYPOINT ["/opt/entrypoint.sh"]
