#! /usr/bin/python
# spark-submit /examples/spark/spark-mysql.py

from initSpark import initspark, hdfsPath
sc, spark, conf = initspark("spark-mysql")
from pyspark.sql.types import *
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

import mysql.connector
import sys

# create a northwind database and regions table in mysql first
# create database northwind;
# use northwind;
# create table regions (RegionID int, RegionName varchar(30));

try:
	cn = mysql.connector.connect(host='localhost', user='root', password='rootpassword')
	cursor = cn.cursor()
	cursor.execute('create database if not exists northwind')
	cn.close()

	cn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='northwind')
	cursor = cn.cursor()
	cursor.execute('drop table if exists regions')
	cn.close()
except:
	pass

print ('read directly from a Hive table that is already there')
spark.sql('select * from regions').show()

print ('read a CSV file into a data frame')
regions = spark.read.csv('file:///examples/northwind/CSVHeaders/regions', header=True) 
regions.show()

# write the data frame into a mysql table
#regions.write.jdbc("jdbc:mysql://localhost/northwind", table='regions', mode = 'append', properties=properties)
regions.write.format("jdbc").options(url="jdbc:mysql://localhost/northwind", driver='com.mysql.jdbc.Driver', dbtable='regions', user='root', password = "rootpassword", mode = "append", useSSL = "false").save()
# select * from regions;

print ('read a mysql table into a data frame')
regions2 = spark.read.format("jdbc").options(url="jdbc:mysql://localhost/northwind", driver="com.mysql.jdbc.Driver", dbtable= "regions", user="root", password="rootpassword").load()
regions2.show()

print ('use the mysql dataframe as a temporary spark table (hiding the one that is in Hive)')
regions2.createOrReplaceTempView("regions")

print ('read territories from a text file')
territories = spark.read.json('file:///examples/northwind/JSON/territories')
territories.createOrReplaceTempView("territories")

print ('run an HQL query in spark on the data populated from the mysql table')
spark.sql("select r.RegionID, r.RegionName, t.TerritoryID, t.TerritoryName from regions as r join territories as t on r.RegionID = t.RegionID ORDER BY r.RegionID, t.TerritoryID").show()
t = spark.sql("select r.RegionID, r.RegionName, collect_set(t.TerritoryName) as TerritoryList from regions as r join territories as t on r.RegionID = t.RegionID GROUP BY r.RegionID, r.RegionName ORDER BY r.RegionID")
t.show()

print ('write the results as a JSON file')
region_territory_path = hdfsPath('region_territory')
t.write.json(region_territory_path)

print ('show regions and territories flattened out')
rt = spark.read.json(region_territory_path)
rt.createOrReplaceTempView("region_territory")
spark.sql("select r.RegionID, r.RegionName, tl as TerritoryName from region_territory as r LATERAL VIEW explode(TerritoryList) exploded_table as tl").show()


