# Most watched movie 58559 Dark Knight input
import collections
import re
from datetime import datetime
from pyspark import SparkConf, SparkContext


def parseline(line):
    fields = line.split()
    movie_id = int(fields[1])
    return movie_id, 1

def parseline2(line):
    fields = line.split(',')
    movie_id = int(fields[1])
    return movie_id, 1

startTime = datetime.now()

host = localhost
if len(sys.argv) > 1:
    host = sys.argv[1]

conf = SparkConf().setMaster(host).setAppName("Most Popular Movie 20m")
sc = SparkContext(conf=conf)

#lines = sc.textFile('../ml-100k/u.data')
lines = sc.textFile('./data/ml-20m/ratings.csv')

header = lines.first()
lines = lines.filter(lambda x: x != header)

#movie_rdd = lines.map(parseline)
movie_rdd = lines.map(parseline2)
movie_group_rdd = movie_rdd.reduceByKey(lambda x,y: x+y)
#movie_group_rdd = movie_rdd.countByKey(lambda x,y: x+y)
movie_reverse_rdd = movie_group_rdd.map(lambda (x, y): (y, x))

max_movie_rdd = movie_reverse_rdd.max()

results = max_movie_rdd

print results

print "Time in seconds", datetime.now() - startTime

