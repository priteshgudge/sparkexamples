import collections
from datetime import datetime
from pyspark import SparkConf, SparkContext


def parseline(line):
    fields = line.split(',')
    station_id = str(fields[0])    
    temp_type = str(fields[2])
    temp = int(fields[3])
    return station_id, temp_type, temp
    
startTime = datetime.now()

conf = SparkConf().setMaster('local').setAppName("MinTemp")
sc = SparkContext(conf=conf)

#lines = sc.textFile("./data/1800.csv")
lines = sc.textFile("hdfs://localhost:9000/books/1801.csv")
tempRDD = lines.map(parseline)
tempFilteredRDD = tempRDD.filter(lambda x: x[1].strip() == "TMIN")
stationtempsRDD = tempFilteredRDD.map(lambda x: (x[0], x[2]))
#print stationtempsRDD.collect()

mintempRDD = stationtempsRDD.reduceByKey(lambda x, y: min(x,y))
#tempbyStation = rdRDD
print mintempRDD.collect()

print "Time in seconds", datetime.now() - startTime

