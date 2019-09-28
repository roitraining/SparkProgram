# pip install pymongo
# pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1

import pymongo
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
classroom = client["classroom"]
if 'classroom' in (x['name'] for x in client.list_databases()):
    client.drop_database('classroom')

people = classroom['people']
name = {"firstname" : "Adam", "personid":4}
x = people.insert_one(name)

names = [{"firstname" : "Betty", "personid":5}
         ,{"firstname" : "Charlie", "personid":6}]
x = people.insert_many(names)

x = people.find()
print (list(x))


spark = SparkSession.builder.appName("myApp")\
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/classroom") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/classroom") \
    .getOrCreate()

df = spark.read.format("mongo").option("uri", "mongodb://127.0.0.1/classroom.people").load()
df.show()

x = sc.parallelize([(7, 'David')])
x1 = spark.createDataFrame(x, schema = ['personid', 'firstname'])
x1.write.format("mongo").options(collection="people", database="classroom").mode("append").save()

df = spark.read.format("mongo").option("uri", "mongodb://127.0.0.1/classroom.people").load()
df.show()

