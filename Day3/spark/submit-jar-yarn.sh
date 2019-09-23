#! /bin/sh
hadoop fs -put /examples/text/shakespeare.txt /
hadoop fs -rm -r /grep_yarn
hadoop jar /examples/java/grepnumwords/target/grepnumwords-1.0.0.jar com.roi.hadoop.grepnumwords.Main hdfs://$HOSTNAME:9000/shakespeare.txt hdfs://$HOSTNAME:9000/grep_yarn king queen sword
hadoop fs -ls /grep_yarn
hadoop fs -cat /grep_yarn/*
