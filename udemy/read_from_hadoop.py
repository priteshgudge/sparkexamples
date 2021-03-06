#hdfs://

import collections
from datetime import datetime
from pyspark import SparkConf, SparkContext


def parseline(line):
    fields = line.split(' ')
    return fields
    
startTime = datetime.now()

conf = SparkConf().setMaster('local').setAppName("MinTemp")
sc = SparkContext(conf=conf)

lines = sc.textFile("hdfs://localhost:9000/books/book1.txt")

aRDD = lines.map(parseline)

print aRDD.take(20)
