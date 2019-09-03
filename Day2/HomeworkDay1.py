#!/usr/bin/env python
# coding: utf-8

# HOMEWORK:
# Find the total expenditure for each city in the CreditCard.csv

# In[1]:


import sys
sys.path.append('/home/student/ROI/SparkProgram')
from initspark import *
sc, spark, conf = initspark()


# In[7]:


cc = sc.textFile('/home/student/ROI/SparkProgram/datasets/finance/CreditCard.csv')
first = cc.first()
cc = cc.filter(lambda x : x != first)
cc.take(10)


# In[8]:


cc = cc.map(lambda x : x.split(',')) 
cc.take(10)


# In[9]:


cc = cc.map(lambda x : ((x[0][1:], x[1][1:-1]), (x[5], float(x[6]))))
print (cc.collect())


# In[11]:


ccf = cc.filter(lambda x : x[1][0] == 'F').map(lambda x : (x[0], x[1][1]))
ccg = ccf.reduceByKey(lambda x, y : x + y)
print (ccg.sortByKey().collect())


# In[ ]:




