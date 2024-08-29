# TODOs

1. Integrate elastic search which consumes messages as a different consumer group from kafka. This will serve the raw log search requests.
2. Integrate the (regex-based) rust normalizer which will publish messages to kafka.
3. Study elastic search to find the use-cases and possible queries to search logs.
4. Study how custom alert rules could be made to run as separate spark processors.
