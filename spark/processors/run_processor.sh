
cd /opt/local/bin/processors

log4j_properties_file="$PWD/log4j.properties"
processor_file_name=$1
spark_master_address=$2


spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$log4j_properties_file" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$log4j_properties_file" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --master $spark_master_address $processor_file_name

