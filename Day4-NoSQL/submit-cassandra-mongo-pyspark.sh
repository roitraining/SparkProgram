#! /bin/sh
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 --conf spark.cassandra.connection.host=127.0.0.1 cassandra-mongo-pyspark.py


