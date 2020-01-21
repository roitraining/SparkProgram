from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *

def initspark(appname = "Test", servername = "local", cassandra="cassandra"):
    conf = SparkConf().set("spark.cassandra.connection.host", cassandra).setAppName(appname).setMaster(servername)
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName(appname).enableHiveSupport().getOrCreate()
    sc.setLogLevel("ERROR")
    return sc, spark, conf

sc, spark, conf = initspark()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "127.0.0.1:2181").option("subscribe", "classroom").load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df.writeStream.format("console").option("truncate","false").start().awaitTermination()
