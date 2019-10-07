#! /bin/sh
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4,com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 kafkaStream2.py

