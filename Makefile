.DEFAULT_GOAL := run

build:
	docker-compose build

start_producer:
	@echo "Waiting 10 seconds to make sure kafka is up..."
	sleep 10
	docker exec -it kafka kafka-console-producer --topic logs --bootstrap-server localhost:9092

run_app:
	docker-compose up -d --build

run: run_app start_producer

pyspark_logs:
	docker logs -f pyspark-client

stop:
	docker compose down -v

help:
	@echo "Usage:"
	@echo "	make build"
	@echo "	make run"
	@echo "	make stop"
	@echo "	make pyspark_logs"


# List kafka topics
# docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092