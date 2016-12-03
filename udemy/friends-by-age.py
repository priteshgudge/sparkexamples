import collections
from datetime import datetime
from pyspark import SparkConf, SparkContext

def parseline(line):
    fields = line.split(',')
    age = int(fields[2])
    no_of_friends = int(fields[3])
    return age, no_of_friends
    

conf = SparkConf().setMaster('local').setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

lines = sc.textFile("./data/fakefriends.csv")


age_friends_tup_list = lines.map(parseline)

intermeditate_map = age_friends_tup_list.mapValues(lambda x: (x,1)) # (33, 385) --> (33, (385,1))

totals_by_age = intermeditate_map.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) #--> (33, (385,1)), (33, (42,1)) --> (33, (427,2))

average_by_age = totals_by_age.mapValues(lambda x: float(x[0])/x[1])

#average_by_age = average_by_age.map(lambda x: (x[1], x[0]))
#results = average_by_age.sort()
#results = sorted(average_by_age.collect(), reverse=True)
#results = map(lambda x: (x[1], x[0]), results)

# http://spark.apache.org/docs/1.1.1/api/python/pyspark.rdd.RDD-class.html
average_by_age_sorted = average_by_age.sortBy(lambda x: x[1], ascending=False, numPartitions=4)
results = average_by_age_sorted.collect()
for result in results:
    print result

