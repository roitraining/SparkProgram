import platform
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *

def initspark(appname = "Test", servername = "local"):
	conf = SparkConf().setAppName(appname).setMaster(servername)
	sc = SparkContext(conf=conf)
	#spark = SQLContext(sc)
	spark = SparkSession \
    	.builder \
    	.appName(appname) \
    	.enableHiveSupport() \
    	.getOrCreate()
	sc.setLogLevel("ERROR")
	return sc, spark, conf



def hdfsPath(folder, hostname = None, port = 9000):
	if hostname == None:
		hostname = platform.node()
	if port == None:
		port = 9000
	if folder == None:
		folder = ''
		
	return 'hdfs://{0}:{1}/{2}'.format(hostname, port, folder)

