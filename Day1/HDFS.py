#!/usr/bin/env python
# coding: utf-8

# The Hadoop File System (HDFS) is a distributed file system that spans across multiple nodes and save file in a cluster. It slices large files into blocks and redundantly saves multiple copies across several nodes in the cluster according to the replication factor chosen for the cluster. 
# To examine the contents of the HDFS cluster you either need to install the hadoop tools on a local machine or ssh into a remote machine that has them installed.
# Try the following commands to see what is currently on the cluster and add new files to it.

# In[1]:


get_ipython().system(' hadoop fs -ls /')


# In[2]:


get_ipython().system(' hadoop fs -put /home/student/ROI/SparkProgram/datasets/northwind/CSV/categories /')


# In[4]:


get_ipython().system(' hadoop fs -ls /')
get_ipython().system(' hadoop fs -ls /categories')


# In[ ]:




