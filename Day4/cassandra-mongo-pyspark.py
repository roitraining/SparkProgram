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

def initspark(appname = "Test", servername = "local", cassandra="cassandra", mongo="mongo"):
    conf = SparkConf().set("spark.cassandra.connection.host", cassandra).setAppName(appname).setMaster(servername)
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName(appname) \
    .config("spark.mongodb.input.uri", mongo) \
    .config("spark.mongodb.output.uri", mongo) \
    .enableHiveSupport().getOrCreate()
    sc.setLogLevel("ERROR")
    return sc, spark, conf

print('*' * 80)

sc, spark, conf = initspark(cassandra = '127.0.0.1', mongo = 'mongodb://127.0.0.1/classroom')

# Python to access a Cassandra cluster through Spark

people = spark.read.format("org.apache.spark.sql.cassandra").options(table="student", keyspace="classroom").load()
print('spark sql query from cassandra')
print('*' * 80)
people.createOrReplaceTempView('people')
people2 = spark.sql('select id, firstname, lastname, email from people LATERAL VIEW EXPLODE(emails) EXPLODED_TABLE AS email')
people2.show()

print ('*' * 80)

df = spark.read.format("mongo").option("uri", "mongodb://127.0.0.1/classroom.people").load()
print ('*' * 80)
print ('mongo from pyspark')
df.show()
print ('*' * 80)

df.createOrReplaceTempView('people')

print ('*' * 80)
print ('mongo spark sql')
spark.sql('select * from people').show()
print ('*' * 80)

