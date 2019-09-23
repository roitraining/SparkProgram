#! /usr/bin/python
# spark-submit --jars /usr/local/mongo-hadoop/mongo-hadoop-core.jar,/usr/local/mongo-hadoop/mongo-hadoop-spark.jar /examples/spark/spark-mongo.py
# pyspark --jars /jars/mongo-hadoop-core.jar,/jars/mongo-hadoop-spark.jar

from initSpark import initspark, hdfsPath
sc, spark, conf = initspark("spark-mongo")
from pyspark.sql.types import *
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

import sys
sys.path.append('/usr/local/mongo-hadoop/mongo-hadoop/spark/src/main/python')
import pymongo_spark
pymongo_spark.activate()

rdd = sc.mongoRDD('mongodb://localhost:27017/northwind.products')
print (rdd.collect())

products = spark.createDataFrame(rdd, "ProductID:int, ProductName:string, UnitPrice:float, CategoryID:int, SupplierID:int")
products.createOrReplaceTempView("products")
products1 = spark.sql('select CategoryID, AVG(UnitPrice) as AvgPrice FROM Products GROUP BY CategoryID')
products1.show()
