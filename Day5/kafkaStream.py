from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

config = SparkConf()
config.setAppName('kafkaStream')

#sc = SparkContext("local[2]", "kafkaStream")
sc = SparkContext("local[2]", conf=config)
sc.setLogLevel('ERROR')
codes = sc.parallelize([(1, 'alpha'), (2, 'beta'), (3, 'delta'), (4, 'gamma')])
codes = getSparkSessionInstance(config).createDataFrame(codes, schema = 'id:int, name:string')
codes.createOrReplaceTempView('codes')

print(codes.collect())

def process(time, rdd):
  try:
    spark = getSparkSessionInstance(rdd.context.getConf())
    rdd1 = rdd.map(lambda x : x[1].split(',')) \
              .map(lambda x : (int(x[0]), float(x[1])))
    df = spark.createDataFrame(rdd1, schema='id:int, amount:float')
    df.createOrReplaceTempView('newdata')
    join = spark.sql('select n.id, c.name, n.amount from newdata as n join codes as c on n.id = c.id')
    join.show()
  except:
    print(rdd.collect())

ssc = StreamingContext(sc, 5)
kafkaStream = KafkaUtils.createStream(ssc, '127.0.0.1:2181', 'spark-streaming', {'classroom':1})
#kafkaStream.pprint()
kafkaStream.foreachRDD(process)



ssc.start()
ssc.awaitTerminationOrTimeout(10000)
ssc.stop()

