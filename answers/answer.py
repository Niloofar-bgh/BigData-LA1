import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader

'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 

We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''

#Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

#Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
    return a + os.linesep

def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
Plain PYTHON implementation

To get started smoothly and become familiar with the assignment's 
technical context (Git, GitHub, pytest, Travis), we will implement a 
few steps in plain Python.
'''

#Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header lines) in
    the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''
    print('test')    
    # ADD YOUR CODE HER
    import numpy as np
    data = []
    with open (filename, 'r') as file: 
        lines = file.readlines()
        for l in lines:
            lread = l.split(',')[0]
            data.append(lread)

    data = np.delete(data,0)
    data = np.asarray([data])

    return len(data[0])
    raise ExceptionException("Not implemented yet")

def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''
      
    # ADD YOUR CODE HERE
    import pandas as pd
    data = pd.read_csv( filename , usecols=['Nom_parc'])
    result =sum(list(data.groupby('Nom_parc').size()))
    return result
	
    raise Exception("Not implemented yet")
def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''

    # ADD YOUR CODE HERE
    import pandas as pd
    from collections import Counter
    test = pd.read_csv(filename)
    storage = (list( Counter(test ['Nom_parc']).keys()))
    storage.pop(0)
    storage =  sorted(storage)
    storage = '\n'.join(storage)+'\n'
    return storage
    raise Exception("Not implemented yet")

def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    
    # ADD YOUR CODE HERE
    import pandas as pd
    data = (pd.read_csv(filename))
    columns = data.columns
    park = data.Nom_parc.tolist()
    park = [x for x in park if str(x) != 'nan']
    d = {}
    park_count = []
    for item in park:
        if item in d:
            d[item] = d.get(item)+1
        else:
            d[item] = 1

    result = ["{0},{1}".format(k, d[k]) for k in sorted(d.keys())]    
    result = ('\n'.join(result)+'\n')
    return result
    raise Exception("Not implemented yet")


def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
   # from collections import Counter
   # import pandas as pd
   # data = (pd.read_csv(filename))
   # columns = data.columns
   # park = data.Nom_parc.tolist()
   # park = [x for x in park if str(x) != 'nan']
   # count = Counter(park)
   # park_top10 = (count.most_common(10))
   # park_top10 = [i[0] for i in park_top10]
   # park_top10 = ('\n'.join(park_top10)+'\n')
   # return park_top10
    from collections import Counter
    import pandas as pd
    data = (pd.read_csv(filename))
    columns = data.columns
    park = data.Nom_parc.tolist()
    park = [x for x in park if str(x) != 'nan']
    count = Counter(park)
    park_top10 = (count.most_common(10))
    park_top10 = ('\n'.join([ str(elem[0])+","+str(elem[1]) for elem in park_top10 ])+'\n')
    return park_top10
    raise Exception("Not implemented yet")

def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''
    from collections import Counter
    import pandas as pd

    data_16 = (pd.read_csv(filename1))
    data_15 = (pd.read_csv(filename2))

    data_16 = list( Counter(data_16['Nom_parc']).keys())
    data_15 = list( Counter(data_15['Nom_parc']).keys())

    data_16.pop(0)
    data_15.pop(0)

    result = list(set(data_16).intersection(data_15))
    result = sorted(result)
    result = ('\n'.join(result)+'\n')
    return result

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

'''
SPARK RDD IMPLEMENTATION

You will now have to re-implement all the functions above using Apache 
Spark's Resilient Distributed Datasets API (RDD, see documentation at 
https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
However, all operations must be re-implemented using the RDD API, you 
are not allowed to simply convert results obtained with plain Python to 
RDDs (this will be checked). Note that the function *toCSVLine* in the 
HELPER section at the top of this file converts RDDs into CSV strings.
'''

# RDD functions

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext


    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    sql_context = SQLContext(sc)

    df = (sql_context.read.format('com.databricks.spark.csv').option("header", "true").load(filename)).rdd 
    return(df.count())

    raise Exception("Not implemented yet")

def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext

    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    sql_context = SQLContext(sc)

    rdd = (sql_context.read.format('com.databricks.spark.csv').option("header", "true").load(filename)).rdd 
    numPark = rdd.map(lambda x: x[6])
    numPark = numPark.filter(lambda x: x is not None).filter(lambda x: x != "")
    return numPark.count()
    raise Exception("Not implemented yet")

def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext

    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    sql_context = SQLContext(sc)
    rdd = (sql_context.read.format('com.databricks.spark.csv').option("header", "true").load(filename)).rdd 
    numPark = rdd.map(lambda x: x[6])
    numPark = numPark.filter(lambda x: x is not None).filter(lambda x: x != "")
    numPark = numPark.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    numPark = numPark.sortByKey().map(lambda x: x[0])
    return toCSVLine(numPark)
    raise Exception("Not implemented yet")

def uniq_parks_counts_rdd(filename):
    '''
    Write a Python script using RDDs that counts the number of trees treated in
    each park and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts_rdd.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext
    
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    sql_context = SQLContext(sc)
    rdd = (sql_context.read.format('com.databricks.spark.csv').option("header", "true").load(filename)).rdd
    numPark = rdd.map(lambda x: x[6])
    numPark = numPark.filter(lambda x: x is not None).filter(lambda x: x != "")
    numPark = numPark.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    numParkCount = numPark.map(lambda x: (x, '')).sortByKey()
    return toCSVLine(numParkCount)
    raise Exception("Not implemented yet")

def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext

    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    sql_context = SQLContext(sc)
    rdd = (sql_context.read.format('com.databricks.spark.csv').option("header", "true").load(filename)).rdd
    numPark = rdd.map(lambda x: x[6])
    numPark = numPark.filter(lambda x: x is not None).filter(lambda x: x != "")
    numPark = numPark.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    numParkCount = numPark.sortByKey()
    sortedNumPark = numParkCount.takeOrdered(10, lambda x: -x[1])
    sortedNumPark = ('\n'.join([str(elem[0])+","+str(elem[1]) for elem in sortedNumPark])+'\n')
    return sortedNumPark
    raise Exception("Not implemented yet")

def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE 
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext
   
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    sql_context = SQLContext(sc)

    rdd_1 = (sql_context.read.format('com.databricks.spark.csv').option("header", "true")\
           .load("frenepublicinjection2016.csv")).rdd 
    numPark_1 = rdd_1.map(lambda x: x[6])
    numPark_1 = numPark_1.filter(lambda x: x is not None).filter(lambda x: x != "")

    rdd_2 = (sql_context.read.format('com.databricks.spark.csv').option("header", "true")\
           .load("frenepublicinjection2015.csv")).rdd 
    numPark_2 = rdd_2.map(lambda x: x[6])
    numPark_2 = numPark_2.filter(lambda x: x is not None).filter(lambda x: x != "")

    intersection = sorted(numPark_1.intersection(numPark_2).collect())
    intersection = ('\n'.join(intersection)+'\n')
    
    return intersection
    raise Exception("Not implemented yet")


'''
SPARK DATAFRAME IMPLEMENTATION

You will now re-implement all the tasks above using Apache Spark's 
DataFrame API (see documentation at 
https://spark.apache.org/docs/latest/sql-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
Note: all operations must be re-implemented using the DataFrame API, 
you are not allowed to simply convert results obtained with the RDD API 
to Data Frames. Note that the function *toCSVLine* in the HELPER 
section at the top of this file also converts DataFrames into CSV 
strings.
'''

# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

'''
DASK IMPLEMENTATION (bonus)

You will now re-implement all the tasks above using Dask (see 
documentation at http://docs.dask.org/en/latest). Outputs must be 
identical to the ones obtained previously. Note: all operations must be 
re-implemented using Dask, you are not allowed to simply convert 
results obtained with the other APIs.
'''

# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''
    print("platypus")

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")
