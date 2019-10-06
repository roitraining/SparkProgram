from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming import DStream

sc = SparkContext("local[2]", "cassandraStream")
sc.setLogLevel('Error')
ssc = StreamingContext(sc, 5)
print(dir(ssc))
records = ssc.cassandraTable('classroom', 'student').select('id', 'first', 'last', 'emails')
dstream = DStream(ssc, records)
dstream.foreachRDD(lambda x : print(rdd.collect()))

ssc.start()
ssc.awaitTerminationOrTimeout(10000)
ssc.stop()

