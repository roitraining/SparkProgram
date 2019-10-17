from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from json import loads

from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('classroom')
session.execute("drop table if exists kafka")
session.execute("create table kafka(id int PRIMARY KEY, name text, amount float, parentid int, parentname text)")

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

config = SparkConf()
config.setAppName('kafkaStream')

def functionToCreateContext():
    sc = SparkContext('local[2]', 'checkpoint')
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint('/tmp/checkpointDirectory')  # set checkpoint directory
    return ssc

ssc = StreamingContext.getOrCreate('/tmp/checkpointDirectory', functionToCreateContext)
kafkaStream = KafkaUtils.createStream(ssc, '127.0.0.1:2181', 'spark-streaming', {'classroom':1})
sc = ssc.sparkContext
codes = sc.parallelize([(1, 'alpha'), (2, 'beta'), (3, 'delta'), (4, 'gamma')])
codes = getSparkSessionInstance(config).createDataFrame(codes, schema = 'id:int, name:string')
codes.createOrReplaceTempView('codes')

print(codes.collect())

def process(time, rdd):
  try:
    spark = getSparkSessionInstance(rdd.context.getConf())
    rdd1 = rdd.map(lambda x : x[1])
    df = spark.read.json(rdd1)
    df.createOrReplaceTempView('newdata')
    join = spark.sql('select n.id, n.name, n.parentid, c.name as parentname, n.amount from newdata as n join codes as c on n.parentid = c.id')
    join.show()
    join.write.format("org.apache.spark.sql.cassandra").options(table="kafka", keyspace="classroom").mode("append").save()
  except Exception as e:
    print('error', e)

kafkaStream.foreachRDD(process)

ssc.start()
ssc.awaitTerminationOrTimeout(10000)
ssc.stop()

