run: rmprom build
	docker-compose up --force-recreate

build:
	cp ../prometheus_monetdb_adapter adapter/
	docker-compose build

rmprom:
	docker ps -a | grep compose_prometheus | awk '{print $$1}' | xargs docker rm -v || true
