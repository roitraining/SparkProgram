#! spark-submit textStream.py localhost 9999
# nc -lk 9999
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# Create a local StreamingContext with two working thread and batch interval of$
sc = SparkContext("local[2]", "textStream")
sc.setLogLevel('Error')
ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream('localhost', 9999)
words = lines.flatMap(lambda line: line.split(' '))
pairs = words.map(lambda w: (w, 1))
wordCount = pairs.reduceByKey(lambda x, y : x + y)
wordCount.pprint()
ssc.start()
ssc.awaitTerminationOrTimeout(10000)
ssc.stop()

