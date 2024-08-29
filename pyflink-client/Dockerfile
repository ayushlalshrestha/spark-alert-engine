FROM flink:1.19-scala_2.12-java11

ENV DEBIAN_FRONTEND=noninteractive

# Setup java environment
RUN apt update && apt install -y openjdk-11-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH
RUN mkdir -p /opt/java/openjdk && ln -s /usr/lib/jvm/java-11-openjdk-arm64 /opt/java/openjdk

# Install necessary python dependencies
RUN apt-get update  && apt-get install -y python3 python3-pip
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN pip install apache-flink==1.19


# Download and copy Kafka connector JAR
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka_2.12/1.14.0/flink-connector-kafka_2.12-1.14.0.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.0/kafka-clients-2.8.0.jar /opt/flink/lib/
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.12/1.14.0/flink-sql-connector-kafka_2.12-1.14.0.jar /opt/flink/lib/
# RUN "start-cluster.sh"

CMD ["bash"]
