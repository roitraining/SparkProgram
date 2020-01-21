create keyspace classroom with replication={'class':'SimpleStrategy', 'replication_factor':'1'};
use classroom;
create table People(PersonID int PRIMARY KEY, FirstName text);
insert into people (PersonID, Firstname) values (1, 'joey');
update people set firstname = 'Joey' where personid = 1;
insert into people (personid, firstname) values (2, 'mike');
insert into people (personid, firstname) values (2, 'Mike');




from initspark import initspark
sc, spark, conf = initspark()
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

pip install cassandra-driver

pyspark --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1

from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('classroom')
rows = session.execute('SELECT PersonID, FirstName FROM people')
print(list(rows))

spark.read.format("org.apache.spark.sql.cassandra").options(table="people", keyspace="classroom").load().show()

x = sc.parallellize([(3, 'Jack')])
x1 = spark.createDataFrame(x, schema = ['personid', 'firstname'])
x1.write.format("org.apache.spark.sql.cassandra").options(table="people", keyspace="classroom").mode("append").save()


https://www.w3schools.com/python/python_mongodb_limit.asp

pip install pymongo

pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1

import pymongo
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
classroom = client["classroom"]

people = classroom['people']
name = {"firstname" : "Adam", "personid":4}
x = people.insert_one(name)

names = [{"firstname" : "Betty", "personid":5}
         ,{"firstname" : "Charlie", "personid":6}]
x = people.insert_many(names)

x = people.find()
print (list(x))


my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/classroom") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/classroom") \
    .getOrCreate()
    
df = spark.read.format("mongo").option("uri", "mongodb://127.0.0.1/classroom.people").load()

people.write.format("mongo").mode("append").save()

x = sc.parallelize([(7, 'David')])
x1 = spark.createDataFrame(x, schema = ['personid', 'firstname'])
x1.write.format("mongo").options(collection="people", database="classroom").mode("append").save()


