import os
import json
import math
import sys
sys.setdefaultencoding('utf-8')
import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from functools import partial
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

if __name__ == '__main__':
	spark = SparkSession.builder.appName('test').getOrCreate()
	sc = spark.sparkContext
	rdd = sc.textFile('/test/data.txt')
	