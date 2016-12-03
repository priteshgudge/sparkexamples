import collections
import re
from datetime import datetime
from pyspark import SparkConf, SparkContext


def parseline(line):
    fields = line.split(' ')
    return fields

REGEX = re.compile(r'\W+', re.UNICODE)

def normalize_words(text):
    return REGEX.split(text)    


startTime = datetime.now()


conf = SparkConf().setMaster('local').setAppName("WordCount")
sc = SparkContext(conf=conf)


#lines = sc.textFile("./data/ml-20m/ratings.csv")
lines = sc.textFile("./data/Book.txt")
#wordsRDD = lines.flatMap(lambda x: x.split())
wordsRDD = lines.flatMap(normalize_words)
#wordCounts = wordsRDD.countByValue()
#wordCountsSorted = wordCounts.sortBy()

# Hard way for count by value
wordsCountRDD = wordsRDD.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# Flipping word, count to count, word

wordsCountFlippedRDD = wordsCountRDD.map(lambda (x, y): (y, x)).sortByKey(ascending=False)

results = wordsCountFlippedRDD.collect()

print "Time in seconds", datetime.now() - startTime

for result in results[:30]:
    count = result[0]
    clean_word = result[1].encode('ascii', 'ignore')
    if clean_word:
        print clean_word, count
