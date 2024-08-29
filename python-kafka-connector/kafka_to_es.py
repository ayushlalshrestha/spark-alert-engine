import json
import logging as log
import sys
import time

from confluent_kafka import Consumer, KafkaException, KafkaError
from elasticsearch import Elasticsearch
from urllib3.exceptions import InsecureRequestWarning
import warnings

warnings.filterwarnings("ignore", category=InsecureRequestWarning)

# Kafka consumer configuration
kafka_config = {
    "bootstrap.servers": "kafka:9092",  # Kafka broker address
    "group.id": "python-kafka-to-es-connector-group-2",
    "auto.offset.reset": "earliest",
}

# Kafka topic
topic = "logs"


def get_elasticsearch_connection() -> Elasticsearch:
    es_host = "https://es01:9200"
    es_username = "hehehe"  # Read this from config (or ENV)
    es_password = "hehehe"  # Read this from config (or ENV)

    es = Elasticsearch(
        [es_host],
        http_auth=(es_username, es_password),
        verify_certs=False,
    )

    # Verify the connection
    if es.ping():
        log.warning("Connected to Elasticsearch")
    else:
        log.warning("Could not connect to Elasticsearch")

    return es


def consume_logs():
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])

    es = get_elasticsearch_connection()

    log.warning("Starting to consume logs...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.error(msg.error())
                    continue
                else:
                    raise KafkaException(msg.error())

            # Parse the Kafka message (assumed to be in JSON format)
            try:
                # log_entry = json.loads(msg.value())
                log_entry = json.loads(msg.value().decode("utf-8").replace("'", '"'))
            except Exception as ex:
                log.error(ex)
                continue

            # Index the log entry into Elasticsearch
            # Elasticsearch configuration
            es.index(index="logs", body=log_entry)
            log.warning(f"Log entry indexed in Elasticsearch: {log_entry}")

    except KeyboardInterrupt:
        sys.exit(0)
    finally:
        consumer.close()


if __name__ == "__main__":
    while True:
        try:
            consume_logs()
        except Exception as ex:
            log.error(f"Kafka to ES; error={ex}")
            time.sleep(5)
