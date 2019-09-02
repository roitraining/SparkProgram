# you should make sure you have spark in your python path as below
# export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
# but if you don't it will append it automatically for this session

import platform, os, sys
from os.path import dirname

if not 'SPARK_HOME' in os.environ and not os.environ['SPARK_HOME'] in sys.path:
    sys.path.append(os.environ['SPARK_HOME']+'/python')

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

def initspark(appname = "Test", servername = "local", cassandra="cassandra"):
    conf = SparkConf().set("spark.cassandra.connection.host", cassandra).setAppName(appname).setMaster(servername)
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName(appname).enableHiveSupport().getOrCreate()
    sc.setLogLevel("ERROR")
    return sc, spark, conf

def hdfsPath(folder, hostname = 'localhost', port = 9000):
    if hostname == None:
        hostname = 'localhost'
        #hostname = platform.node()
    if port == None:
        port = 9000
    if folder == None:
        folder = ''
    return 'hdfs://{0}:{1}/{2}'.format(hostname, port, folder)

if __name__ == '__main__':
    sc, spark, conf = initspark()

