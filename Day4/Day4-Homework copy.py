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

sc, spark, conf = initspark(cassandra = '127.0.0.1', mongo = 'mongodb://127.0.0.1/classroom')

def prepNoSQL():
	# Prepare Cassandra Table
	from cassandra.cluster import Cluster
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()
	session = cluster.connect('classroom')
	session.execute("drop table if exists products")
	session.execute("create table products(productid int PRIMARY KEY, productname text, unitprice float)")

	# Write products to Cassandra Table
	p = spark.read.json('/home/student/ROI/SparkProgram/datasets/northwind/JSON/products')
	p.createOrReplaceTempView('products')
	p1 = spark.sql('select ProductID as productid, ProductName as productname, UnitPrice as unitprice from products')
	p1.show()
	print(p1)
	p1.write.format("org.apache.spark.sql.cassandra").options(table="products", keyspace="classroom").mode('append').save()

	# Drop Mongo Collection
	import pymongo
	client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
	classroom = client["classroom"]
	if 'orders' in classroom.collection_names():
	    classroom['orders'].drop()

	# Write orders to Mongo Collection
	o = spark.read.json('../Day3/Orders_LineItems.json')
	o.show()
	o.write.format('mongo').option('uri', 'mongodb://127.0.0.1/classroom.orders').save()

# you only need to run this once, so comment it out to save time after you've run it one time
prepNoSQL()

"""
Begin Solution
Write a query that will read the mongo Orders collection and the Cassandra Products table 
flatten orders to get to the order details so you can join it to products to get the product names matched to each product id.
Bonus: Regroup the data by Product with the order information grouped under it.
"""

