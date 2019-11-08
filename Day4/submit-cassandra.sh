#! /bin/sh
1. In order to process unstructure text you could use:
   a. nltk
   b. pyspark.ml.feature
 * c. a & b
   d. None of the above

2. To avoid multiple recomputes of a DataFrame you could use:
   a. pin
*  b. persist
   c. createOrReplaceTempTable
   d. memCache

3. In order to transform a new set of values to make a prediction you would first convert the data using:
   a. a custom function
   b. predict method
   c. transform using a trained model object
 * d. transform with a trained pipeline  

 4. In order to create a global counter that all workers could share:
   a. use a python global variable
 * b. use a sc.accumulator
   c. use a sc.counter
   d. this cannot be done in Spark

5. In order to use text data in a model object you:
* a. convert it to a features vector with IDF
  b. use nltk to break up the sentences into words
  c. simply convert the DataFrame to an RDD first
  d. use a WordCloud