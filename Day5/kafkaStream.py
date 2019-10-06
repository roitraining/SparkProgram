from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext("local[2]", "kafkaStream")
sc.setLogLevel('ERROR')
ssc = StreamingContext(sc, 5)
kafkaStream = KafkaUtils.createStream(ssc, '127.0.0.1:2181', 'spark-streaming', {'classroom':1})
kafkaStream.count().pprint()

ssc.start()
ssc.awaitTerminationOrTimeout(10000)
ssc.stop()

