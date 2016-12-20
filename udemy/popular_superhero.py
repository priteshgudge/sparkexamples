# Most watched movie
import collections
import re
from datetime import datetime
from pyspark import SparkConf, SparkContext


def parseline2(line):
    fields = line.split()
    hero_id = int(fields[0].strip())
    return hero_id, len(fields) - 1

def parseName(line):
    fields = line.split('\"')
    return int(fields[0]), fields[1].encode('utf-8')       

startTime = datetime.now()

conf = SparkConf().setMaster('local').setAppName("PopularHero")
sc = SparkContext(conf=conf)

name_rdd = sc.textFile('./data/Marvel-Names.txt').map(parseName)


#lines = sc.textFile('../ml-100k/u.data')
lines = sc.textFile('./data/Marvel-Graph.txt', minPartitions=4)
print lines.getNumPartitions()

friend_rdd = lines.map(parseline2)

reduced_friend_rdd = friend_rdd.reduceByKey(lambda x, y : x + y)

flipped_popular = reduced_friend_rdd.map(lambda (x, y): (y,x))

most_popular = flipped_popular.max()

#print name_rdd.lookup(most_popular[1])
#print "Total_time without count", datetime.now() - startTime 
print name_rdd.lookup(most_popular[1]), 'Friends',most_popular[0]

print "Total_time with count", datetime.now() - startTime 
