from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# Create a local StreamingContext with two working thread and batch interval of$
sc = SparkContext("local[2]", "fileStream")
sc.setLogLevel('Error')
ssc = StreamingContext(sc, 5)
lines = ssc.textFileStream('stream')
words = lines.flatMap(lambda line: line.split(' '))
pairs = words.map(lambda w: (w, 1))
wordCount = pairs.reduceByKey(lambda x, y : x + y).transform(lambda x : x.sortBy(lambda x : -x[1]))
wordCount.pprint()
ssc.start()
ssc.awaitTerminationOrTimeout(10000)
ssc.stop()

