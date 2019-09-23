#! /usr/bin/python

# submit the premade jar to YARN
# hadoop jar grepnumwords/target/grepnumwords-1.0.0.jar com.roi.hadoop.grepnumwords.Main hdfs://$HOSTNAME:9000/shakespeare.txt hdfs://$HOSTNAME:9000/grep_yarn king queen sword
# submit the premade jar to Spark
# spark-submit --class com.roi.hadoop.grepnumwords.Main grepnumwords/target/grepnumwords-1.0.0.jar hdfs://$HOSTNAME:9000/shakespeare.txt hdfs://$HOSTNAME:9000/grep_spark king queen sword
# submit to Spark as a native Spark program
# spark-submit spark-text.py


from initSpark import initspark, hdfsPath
sc, spark, conf = initspark("spark-text")
from pyspark.sql.types import *
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

lines = sc.textFile(hdfsPath('shakespeare.txt'))
lines2 = lines.map(lambda x : (len(x.split(' ')), x.split(' ')))
lines3 = lines2.flatMapValues(lambda x : x)
searchwords = sc.parallelize([('king', 1), ('queen', 1), ('sword', 1)])
lines4 = lines3.map(lambda x : (x[1], x[0]))
join1 = searchwords.join(lines4)
#path = 'hdfs://{0}:{1}/{2}'.format(hostname, port, 'spark-text')
counts = join1.reduceByKey(lambda x, y : x if x[1] > y[1] else y).map(lambda x : (x[0], x[1][1]))
counts.collect()
counts.saveAsTextFile(hdfsPath('spark-text'))
print "run the following command:\nhadoop fs -cat /spark-text/*"

