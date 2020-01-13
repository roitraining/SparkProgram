#!/usr/bin/env python

# HOMEWORK:
# Find the total expenditure by women for each city in the CreditCard.csv

import sys
sys.path.append('/class')
from initspark import *
sc, spark, conf = initspark()

cc = sc.textFile('/class/datasets/finance/CreditCard.csv')
first = cc.first()
cc = cc.filter(lambda x : x != first)

cc = cc.map(lambda x : x.split(',')) 

cc = cc.map(lambda x : ((x[0][1:], x[1][1:-1]), (x[5], float(x[6]))))

ccf = cc.filter(lambda x : x[1][0] == 'F').map(lambda x : (x[0], x[1][1]))
ccg = ccf.reduceByKey(lambda x, y : x + y)
print('First Ten Cities')
print(ccg.sortByKey().take(10))
print('Top ten cities')
print(ccg.sortBy(lambda x : x[1], ascending = False).take(10))

