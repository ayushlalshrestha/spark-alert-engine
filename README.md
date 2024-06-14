
# Steps to test alerts from spark streaming with logs from Kafka
1. Build and start all the containers
    `docker-compose up -d --build`

2. Go to the shell of the kafka container and produce messages through kafka console producer
    ```
    docker exec -it kafka /bin/bash
    kafka-console-producer --topic logs --bootstrap-server localhost:9092
    > {"timestamp": 1718228602, "username": "ayush"}
    ```

3. Get the address of the spark-master by looking at the logs from 
    `docker logs spark-master`

4. Go to the shell of the spark-master container and submit a spark job that processes an alert. The alert fires if more than 2 messages are recevied for the same user within a time window of 10 minutes.
    ```
    docker exec -it spark-master /bin/bash
    source /opt/local/bin/processors/run_processor.sh timerange_alert_processor.py spark://172.20.0.3:7077
    ```

5. In the console producer shell, send logs for different users at different timestamps to observe how & when the alert fires.
    ```
    > {"timestamp": 1718228602, "username": "ayush"}
    > {"timestamp": 1718228603, "username": "ayush"}
    > {"timestamp": 1718228604, "username": "ayush"}
    ```
