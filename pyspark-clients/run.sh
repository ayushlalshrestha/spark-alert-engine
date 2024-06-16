

log4j_properties_file="/usr/local/bin/pyspark-clients/log4j.properties"
processor_client_program="/usr/local/bin/pyspark-clients/$1"
spark_master_address=$SPARK_MASTER_URL

echo $processor_client_program
echo $SPARK_MASTER_URL


spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$log4j_properties_file" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$log4j_properties_file" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --master $spark_master_address $processor_client_program
