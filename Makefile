.DEFAULT_GOAL := run

build:
	docker-compose build

connect_kafka:
	@echo "Waiting 10 seconds to make sure kafka is up..."
	sleep 10
	docker exec -it kafka kafka-topics --create --topic connect-offsets --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 --config cleanup.policy=compact
	docker exec -it kafka kafka-topics --create --topic connect-configs --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 --config cleanup.policy=compact
	docker exec -it kafka kafka-topics --create --topic connect-status --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 --config cleanup.policy=compact
	docker exec -it kafka kafka-topics --create --topic logs --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
	docker exec -it kafka kafka-console-producer --topic logs --bootstrap-server localhost:9092

start_producer:
	docker exec -it kafka kafka-console-producer --topic logs --bootstrap-server localhost:9092		

run_app:
	# docker-compose up -d --build
	docker-compose up -d --build zookeeper kafka setup es01 kibana kafka-to-es pyflink-client

run: run_app connect_kafka

pyspark_logs:
	docker logs -f pyspark-client

stop:
	docker compose down -v
	rm -rf data/es-data/* 
	rm -rf data/kafka-data/*
	rm -rf data/zk-data/*

help:
	@echo "Usage:"
	@echo "	make build"
	@echo "	make run"
	@echo "	make stop"
	@echo "	make pyspark_logs"
