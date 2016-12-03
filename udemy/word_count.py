import collections
from datetime import datetime
from pyspark import SparkConf, SparkContext


def parseline(line):
    fields = line.split(' ')
    return fields
    
startTime = datetime.now()


conf = SparkConf().setMaster('local').setAppName("WordCount")
sc = SparkContext(conf=conf)

#lines = sc.textFile("./data/ml-20m/ratings.csv")
#lines = sc.textFile("hdfs://localhost:9000/moviedata/ratings.csv")
#wordsRDD = lines.flatMap(lambda x: x.split(','))

lines = sc.textFile("./data/Book.txt")
wordsRDD = lines.flatMap(lambda x: x.split())


print wordsRDD.count()
#print wordsRDD.collect()


print "Time in seconds", datetime.now() - startTime


