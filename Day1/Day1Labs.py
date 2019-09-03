#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
sys.path.append('/home/student/ROI/SparkProgram')
from initspark import *
sc, spark, conf = initspark()


# LAB: Put the regions folder found in /home/student/ROI/SparkProgram/datasets/northwind/CSV/regions into HDFS. Read it into an RDD and convert it into a tuple shape

# In[4]:


get_ipython().system(' hadoop fs -put /home/student/ROI/SparkProgram/datasets/northwind/CSV/regions /regions')


# In[6]:


regions = sc.textFile('hdfs://localhost:9000/regions')
regions = regions.map(lambda x : x.split(',')).map(lambda x : (int(x[0]), x[1]))
print (regions.collect())


# LAB: Try to sort region by name and descending order by ID

# In[9]:


print (regions.sortByKey(ascending = False).collect())


# In[10]:


print (regions.sortBy(lambda x : x[1]).collect())


# LAB: Load territories into HDFS and join it to regions

# In[11]:


get_ipython().system(' hadoop fs -put /home/student/ROI/SparkProgram/datasets/northwind/CSV/territories /territories')


# In[12]:


territories = sc.textFile('hdfs://localhost:9000/territories')
territories = territories.map(lambda x : x.split(',')).map(lambda x : (int(x[0]), x[1], int(x[2])))
print (territories.collect())


# In[19]:


region_territories = regions.join(territories.map(lambda x : (x[2], (x[0],x[1]))))
#print (region_territories.collect())
region_territories = region_territories.map(lambda x : (x[0], (x[1][0], *x[1][1])))
print (region_territories.collect())


# LAB: Use the territories RDD to count how many territories are in each region. Display the results in regionID order and then descending order based on the counts

# In[21]:


region_count = territories.map(lambda x : (x[2], 1)).reduceByKey(lambda x, y: x + y)
print(region_count.sortByKey().collect())


# In[22]:


print(region_count.sortBy(lambda x : x[1], ascending = False).collect())


# In[ ]:




