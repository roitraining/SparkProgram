#!/usr/bin/env python
# coding: utf-8

# Create the Spark context to start a session and connect to the cluster

# In[3]:


import sys
sys.path.append('/home/student/ROI/SparkProgram')
from initspark import *
sc, spark, conf = initspark()


# Read a text file from the local file system

# In[134]:


shake = sc.textFile('/home/student/ROI/SparkProgram/datasets/text/shakespeare.txt')
print(shake.count())
print(shake.take(10))


# Use the map method to apply a function call on each element

# In[135]:


shake2 = shake.map(str.upper)
shake2.take(10)


# Using the split method you get a list of lists

# In[138]:


shake3 = shake.map(lambda x : x.split(' '))
shake3.take(10)


# In[ ]:


The flatMap method flattens the inner list to return one big list of strings instead


# In[139]:


shake4 = shake.flatMap(lambda x : x.split(' '))
shake4.take(20)


# Parallelize will load manually created data into the spark cluster into an RDD

# In[92]:


r = sc.parallelize(range(1,11))
print(r.collect())
print(r.take(5))


# Load a folder stored on HDFS

# In[93]:


sc.textFile('hdfs://localhost:9000/categories').collect()


# Use the helper function to point to the HDFS URI

# In[94]:


cat = sc.textFile(hdfsPath('categories'))
print(cat.takeOrdered(5))
print(cat.top(5))
print(cat.takeSample(False,5))
cat.foreach(lambda x : print(x.upper)) # does not display properly in notebook


# Save the results in an RDD to disk. Note how it makes a folder and filles it with as many files as there are nodes solving the problem. Also you must make sure that the folder does not exist or it throws an exception.

# In[142]:


get_ipython().system(' rm -r /home/student/file1.txt')
cat.saveAsTextFile('/home/student/file1.txt')


# In[96]:


print(cat.map(str.upper).collect())


# Parse the string into a tuple to resemble a record structure

# In[97]:


cat1 = cat.map(lambda x : tuple(x.split(',')))
cat1 = cat1.map(lambda x : (int(x[0]), x[1], x[2]))
cat1.take(10)


# LAB: Put the regions folder found in /home/student/ROI/SparkProgram/datasets/northwind/CSV/regions into HDFS. Read it into an RDD and convert it into a tuple shape

# In[ ]:





# Convert the tuple into a dictionary as an alternative form

# In[98]:


cat2 = cat1.map(lambda x : dict(zip(['CategoryID', 'Name', 'Description'], x)))
cat2.take(10)


# You can chain multiple transformations together to do it all in one step.

# In[99]:


cat2 = cat.map(lambda x : tuple(x.split(',')))       .map(lambda x : (int(x[0]), x[1], x[2]))       .map(lambda x : dict(zip(['CategoryID', 'Name', 'Description'], x)))
cat2.take(10)


# The filter method takes a lambda that returns a True or False

# In[100]:


cat1.filter(lambda x : x[0] <= 5).collect()


# The filter expressions can be more complicated

# In[101]:


cat2.filter(lambda x : x['CategoryID'] % 2 == 0 and 'e' in x['Name']).collect()


# The sortBy method returns an expression that is used to sort the data

# In[102]:


cat1.sortBy(lambda x : x[2]).collect()


# sortBy has an option ascending parameter to sort in reverse order

# In[103]:


cat1.sortBy(lambda x : x[0], ascending = False).collect()


# LAB: Try to sort region by name and descending order by ID

# In[ ]:





# Reshape categories from a tuple of 3 elements like (1, 'Beverages', 'Soft drinks') to a tuple with two elements (key, value) like (1, ('Beverages', 'Soft drinks'))

# In[104]:


cat3 = cat1.map(lambda x : (x[0], (x[1], x[2])))
cat3.collect()


# The sortByKey method does not require a function as a parameter if the data is structured into a tuple of the shape (key, value)

# In[105]:


cat3.sortByKey(ascending=False).collect()


# Read in another CSV file

# In[106]:


prod = shake = sc.textFile('/home/student/ROI/SparkProgram/datasets/northwind/CSV/products')
print(prod.count())
prod.take(4)


# Split it up and just keep the ProductID, ProductName, CategoryID, Price, Quantity values

# In[107]:


prod1 = prod.map(lambda x : x.split(',')).map(lambda x : (int(x[0]), x[1], int(x[3]), float(x[5]), int(x[6])))
prod1.take(5)


# Reshape it to a key value tuple

# In[108]:


prod2 = prod1.map(lambda x : (x[2], (x[0], x[1], x[3], x[4])))
prod2.take(5)


# In[109]:


cat3.collect()


# Both c3 and prod2 are in key value tuple format so they can be joined to produce a new tuple of (key, (cat, prod))

# In[113]:


joined = cat3.join(prod2)
joined.sortByKey().take(15)


# LAB: Load territories into HDFS and join it to regions

# In[ ]:





# The groupBy methods are seldom used but they can produce hierarchies where children records are embedded inside a parent

# In[121]:


list(group1.take(1)[0][1])


# In[115]:


group1 = prod2.groupByKey()
group1.take(3)


# In[143]:


group2 = [(key, list(it)) for key, it in group1.collect()]
for k,v in group2:
    print ('Key:', k)
    for x in v:
        print(x)
#print (group2)


# The reduce methods take a function as a parameter that tells spark how to accumulate the values for each group. The function takes two parameters, the first is the accumulated value and the second is the next value in the list. 

# In[141]:


shake4.map(lambda x : (x, 1)).reduceByKey(lambda x, y : x + y).sortBy(lambda x : x[1], ascending = False).take(10)


# LAB: Use the territories RDD to count how many territories are in each region. 
# Display the results in regionID order and then descending order based on the counts

# In[ ]:





# In this example we are adding up all the prices for each categoryID

# In[130]:


red1 = prod2.map(lambda x : (x[0], x[1][2])).reduceByKey(lambda x, y: x + y)
red1.collect()


# To accumulate more than one value, use a tuple to hold as many values as you want to aggregate

# In[132]:


red1 = prod2.map(lambda x : (x[0], (x[1][2], x[1][3], 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
red1.collect()


# Some python magic can make things easier in the long run.
# Named tuples make accessing the elements of the row easier
# Unpacking using the * is a neat python trick that is widely used
# datetime has function to convert a string into a date

# In[29]:


mort = sc.textFile('/home/student/ROI/SparkProgram/datasets/finance/30YearMortgage.csv')
head = mort.first()
mort = mort.filter(lambda x : x != head)


# In[36]:


from datetime import date, datetime
from collections import namedtuple
Rate = namedtuple('Rate','date fed_fund_rate avg_rate_30year')
mort1 = mort.map(lambda x : Rate(*(x.split(','))))
mort2 = mort1.map(lambda x : Rate(datetime.strptime(x.date, '%Y-%m').date(), float(x.fed_fund_rate), float(x.avg_rate_30year)))
mort2.take(5)


# In[37]:


mort2.filter(lambda x : x.fed_fund_rate > .1 ).collect()


# HOMEWORK:
# Find the total expenditure for each city in the CreditCard.csv made by women

# In[ ]:




