#! /usr/bin/python
# spark-submit --jars "/usr/local/spark/jars/spark-xml.jar" /examples/spark/spark2.py

from initSpark import initspark, hdfsPath
sc, spark, conf = initspark("spark-xml")
from pyspark.sql.types import *
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

products = spark.read.csv('/examples/northwind/CSVHeaders/products/products.csv', header=True) 
products.write \
    .format("com.databricks.spark.xml") \
    .option("rootTag", "products") \
    .option("rowTag", "product") \
    .save("/examples/spark/results/products_xml")

products2 = spark.read \
    .format("com.databricks.spark.xml") \
    .options(rootTag="products", rowTag="product") \
    .load("/examples/spark/results//products_xml")

productSchema = StructType([ \
    StructField("ProductID", IntegerType(), True), \
    StructField("ProductName", StringType(), True), \
    StructField("SupplierID", IntegerType(), True), \
    StructField("CategoryID", IntegerType(), True), \
    StructField("UnitPrice", DoubleType(), True), \
    StructField("UnitsInStock", IntegerType(), True), \
    StructField("UnitsOnOrder", IntegerType(), True)])

products3 = spark.read \
    .format('com.databricks.spark.xml') \
    .options(rootTag="products", rowTag="product") \
    .load("/examples/spark/results/products_xml", schema = productSchema)
    
products.createOrReplaceTempView("products")
products4 = spark.sql("select ProductID, ProductName, SupplierID, CategoryID, UnitPrice, named_struct('InStock', UnitsInStock, 'OnOrder', UnitsOnOrder) as Units FROM products")
products4.write \
    .format("com.databricks.spark.xml") \
    .option("rootTag", "products") \
    .option("rowTag", "product") \
    .save("/examples/spark/results/products4_xml")
    
    
products4.createOrReplaceTempView("products4")
products5 = spark.sql('select ProductID, ProductName, SupplierID, CategoryID, UnitPrice, Units.InStock AS UnitsInStock, Units.OnOrder as UnitsOnOrder FROM products4')
products5.show()



