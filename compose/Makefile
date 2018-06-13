run: rmprom build
	docker-compose up --force-recreate

build:
	cp ../main adapter/
	docker-compose build

rmprom:
	docker ps -a | grep compose_prometheus | awk '{print $$1}' | xargs docker rm -v
