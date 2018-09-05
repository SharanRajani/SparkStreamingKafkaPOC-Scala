# Apache Kafka Producer and Consumer in which uses Spark-Streaming and Avro Serialization written in Scala.


Steps for execution and corresponding explanation:

1. Download Confluent Open Source from https://www.confluent.io/download/ (Tested on v5.0).
2. Extract it and inside the directory, run the following command: bin/confluent start
3. This will start Kafka, Schema Registry, Zookeeper etc.
4. Run mvn clean install
5. Run Prod.scala which is a Kafka Producer which generates (Key,Value)
6. Key is a Random Integer and Value is a Randomly produced True or False value.
7. Run Cons.scala which is a Kafka Consumer which creates a DStream for consuming data produced every 2 secs in Kafka.
8. It calculates number of True and False values generated every 2 secs using map() and reduceByKey() functions. 
