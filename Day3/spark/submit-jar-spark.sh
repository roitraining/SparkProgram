#! /bin/sh
hadoop fs -put /examples/text/shakespeare.txt /
hadoop fs -rm -r /grep_spark
spark-submit --class com.roi.hadoop.grepnumwords.Main /examples/java/grepnumwords/target/grepnumwords-1.0.0.jar hdfs://$HOSTNAME:9000/shakespeare.txt hdfs://$HOSTNAME:9000/grep_spark king queen sword
hadoop fs -ls /grep_spark
hadoop fs -cat /grep_spark/*
