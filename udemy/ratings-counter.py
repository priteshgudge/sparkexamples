from datetime import datetime
startTime = datetime.now()

#do something

from pyspark import SparkConf, SparkContext
import collections

#import pdb; pdb.set_trace()
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("../ml-100k/u.data")
#print lines
ratings = lines.map(lambda x: x.split()[2])
#print ratings
result = ratings.countByValue()
#print result
#print sorted(result.items())
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.iteritems():
    print "%s %i"% (key, value)

print "Time in seconds", datetime.now() - startTime

