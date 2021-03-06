# Most watched movie
import collections
import re
from datetime import datetime
from pyspark import SparkConf, SparkContext


def parseline2(line):
    fields = line.split(',')
    movie_id = int(fields[1])
    return movie_id, 1

def load_movie_names(file_name):
    movie_names = {}    
    count = 0
    with open(file_name, 'r') as f:
        for line in f:
            if count == 0:
                count = 1
                continue            
            fields = line.split(',')
            movie_names[int(fields[0])] = fields[1]
    return movie_names

startTime = datetime.now()

conf = SparkConf().setMaster('local').setAppName("WordCount")
sc = SparkContext(conf=conf)

#lines = sc.textFile('../ml-100k/u.data')
lines = sc.textFile('./data/ml-20m/ratings.csv', minPartitions=4)
print lines.getNumPartitions()

movie_names = load_movie_names('./data/ml-20m/movies.csv')
#print movie_names
name_dict = sc.broadcast(movie_names)


header = lines.first()
lines = lines.filter(lambda x: x != header)

movie_rdd = lines.map(parseline2)

# rdd is movie_id, 1
movie_group_rdd = movie_rdd.reduceByKey(lambda x,y: x+y)

# converting to num, movie_id
movie_reverse_rdd = movie_group_rdd.map(lambda (x, y): (y, x))

# sorting num, movie_id
sorted_movies_rdd = movie_reverse_rdd.sortByKey()

#converting to movie_name, num
movie_names_rdd = sorted_movies_rdd.map(lambda x: (name_dict.value[x[1]], x[0]))

results = movie_names_rdd.collect()[-50:]

print "Time in seconds", datetime.now() - startTime

for result in results:
    print result[0], result[1]
