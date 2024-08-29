# Simple threat detector and log manager with Kafka, Spark & Elasticsearch.

## Steps to test alerts from spark streaming with logs from Kafka
The alert-engine (pyspark-client processor) watches if there are more than 2 logs with the same username within a time-window of 10 minutes.
1. Start all the containers & go into kafka message producer

    make run


2. Send logs for different usernames and timestamp values.
    ```
    > {"timestamp": 1708228602, "username": "ayush"}
    > {"timestamp": 1718381882, "username": "snoie"}
    > {"timestamp": 1718381882, "username": "snoie"}
    > {"timestamp": 1718381882, "username": "snoie"}
    ```

3. See the logs of any alert being fired from the messages you send

    make pyspark_logs 


## Notes
If the alert engine (pyspark processor) is failing, make sure the kafka topic is initialized and then restart the pyspark program.

    > docker exec -it kafka kafka-topics --create --topic logs --bootstrap-server localhost:9092
    > docker-compose up -d pyspark-client

