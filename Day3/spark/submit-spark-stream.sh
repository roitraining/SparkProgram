#! /bin/sh
hadoop fs -mkdir /stream
spark-submit hdfs-stream-count.py hdfs://$HOSTNAME:9000/stream

