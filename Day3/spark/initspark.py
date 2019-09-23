# you should make sure you have spark in your python path as below
# export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
# but if you don't it will append it automatically for this session

import platform, os, sys
from os.path import dirname

if not os.environ['SPARK_HOME'] in sys.path:
   sys.path.append(os.environ['SPARK_HOME']+'/python')

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

def initspark(appname = "Test", servername = "local", enableHiveSupport = False):
	conf = SparkConf().setAppName(appname).setMaster(servername)
	sc = SparkContext(conf=conf)
	spark = SparkSession.builder.appName(appname)
	if enableHiveSupport:
	   spark = spark.enableHiveSupport()
	spark = spark.getOrCreate()
	sc.setLogLevel("ERROR")
	return sc, spark, conf


def hdfsPath(folder, hostname = None, port = 9000):
	if hostname == None:
		hostname = platform.node()
	if port == None:
		port = 9000
	if folder == None:
		folder = ''
	return 'hdfs://{0}:{1}/{2}'.format(hostname, port, folder)


sc, spark, conf = initspark()
