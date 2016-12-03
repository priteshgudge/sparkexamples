import collections
import re
from datetime import datetime
from pyspark import SparkConf, SparkContext


def parseline(line):
    fields = line.split(',')
    cust_id = int(fields[0])
    amount_spent = float(fields[2])
    return cust_id, amount_spent

startTime = datetime.now()

conf = SparkConf().setMaster('local').setAppName("WordCount")
sc = SparkContext(conf=conf)

lines = sc.textFile('./data/customer-orders.csv')

cust_spend_rdd = lines.map(parseline)

cust_spend_acc = cust_spend_rdd.reduceByKey(lambda x, y: x + y)

cust_spend_acc_sorted = cust_spend_acc.sortBy(lambda x: x[1],ascending=False)

results = cust_spend_acc_sorted.collect()

for result in results:
    print result[0], result[1]


