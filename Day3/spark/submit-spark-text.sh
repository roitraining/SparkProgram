#! /bin/sh
hadoop fs -put /examples/text/shakespeare.txt /
hadoop fs -rm -r /grep_spark1
spark-submit spark-text.py
hadoop fs -ls /spark-text
hadoop fs -cat /spark-text/*
