import platform, os, sys
from os.path import dirname

if not 'SPARK_HOME' in os.environ and not os.environ['SPARK_HOME'] in sys.path:
    sys.path.append(os.environ['SPARK_HOME']+'/python')

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

CASSANDRA_IP=os.getenv('CASSANDRA1')
print(CASSANDRA_IP)

if CASSANDRA_IP is None:
    CASSANDRA_IP = '172.18.0.2'

def initspark(appname = "Test", servername = "local", cassandra="cassandra", mongo="mongo"):
    conf = SparkConf().set("spark.cassandra.connection.host", cassandra).setAppName(appname).setMaster(servername)
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName(appname) \
    .config("spark.mongodb.input.uri", mongo) \
    .config("spark.mongodb.output.uri", mongo) \
    .enableHiveSupport().getOrCreate()
    sc.setLogLevel("ERROR")
    return sc, spark, conf

sc, spark, conf = initspark(cassandra = CASSANDRA_IP, mongo = 'mongodb://127.0.0.1/classroom')

def prepNoSQL():
	# Prepare Cassandra Table
	from cassandra.cluster import Cluster
	cluster = Cluster([CASSANDRA_IP])
	session = cluster.connect()
	session = cluster.connect('classroom')
	session.execute("drop table if exists products")
	session.execute("create table products(productid int PRIMARY KEY, productname text, unitprice float)")

	# Write products to Cassandra Table
	p = spark.read.json('/class/datasets/northwind/JSON/products')
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
# prepNoSQL()

"""
Begin Solution
Write a query that will read the mongo Orders collection and the Cassandra Products table 
flatten orders to get to the order details so you can join it to products to get the product names matched to each product id.
Bonus: Regroup the data by Product with the order information grouped under it.
"""

orders = spark.read.format('mongo').option('uri', 'mongodb://127.0.0.1/classroom.orders').load()
products = spark.read.format("org.apache.spark.sql.cassandra").options(table="products", keyspace="classroom").load()

orders.createOrReplaceTempView('orders')
products.createOrReplaceTempView('products')

orders.show()
products.show()

def tempViewFromQuery(viewname, query):
   """
   Helper function to make a dataframe from a query and also create a view in one step
   """
   df = spark.sql(query)
   df.createOrReplaceTempView(viewname)
   return df


flatOrdersSQL = """SELECT customerid, orderid, orderdate, line.productid AS productid
       , line.quantity AS quantity, line.unitprice AS pricepaid 
       FROM orders LATERAL VIEW EXPLODE(lineitems) EXPLODED_TABLE AS line"""
flatOrders = tempViewFromQuery('flatorders', flatOrdersSQL)
flatOrders.show()

joinedOrdersSQL = """SELECT f.customerid, f.orderid, f.orderdate, f.productid
       , f.quantity, f.pricepaid, p.productname 
       FROM flatorders AS f 
       JOIN products as p ON f.productid = p.productid 
       ORDER BY f.productid, f.orderid"""
joinedOrders = tempViewFromQuery('joinedorders', joinedOrdersSQL)
joinedOrders.show()

groupByProductSQL = """SELECT productid, productname
       , collect_list(named_struct("customerid", customerid, "orderid", orderid, "orderdate"
       , orderdate, "productid", productid, "quantity", quantity, "pricepaid", pricepaid)) 
       as orders 
       FROM joinedorders 
       GROUP BY productid, productname"""
groupByProduct = tempViewFromQuery('groupbyproducts', groupByProductSQL)
from pyspark.sql.functions import expr
groupByProduct.withColumn('ordercount', expr('size(orders)')).show()

