import collections
import re
from datetime import datetime
from pyspark import SparkConf, SparkContext


STARTSUPERHERO = 1234 #5306
ENDSUPERHERO = 14 #14

def convertToBFS(line):
    fields = line.split()    
    hero_id = int(fields[0])
    friends_list = map(lambda x: int(x) ,fields[1:])
    color = 'WHITE'
    distance = 9999
    if hero_id == STARTSUPERHERO:
        color = 'GRAY'
        distance = 0
    return hero_id, (friends_list, color, distance)


startTime = datetime.now()

conf = SparkConf().setMaster('local').setAppName("DegOfSeperation")
sc = SparkContext(conf=conf)
hitcounter = sc.accumulator(0)

def create_starting_rdd():
    lines = sc.textFile('./data/Marvel-Graph.txt', minPartitions=4)
    
    superhero_rdd = lines.map(convertToBFS)
    return superhero_rdd

def bfs_mapper(node):
    results = []
    hero_id, (friend_list, color, distance) = node
    if color == 'GRAY':
      
        n_distance = distance + 1
        n_color = 'GRAY'
        for friend in friend_list:
            if ENDSUPERHERO == friend:
                hitcounter.add(1)
            new_entry = (friend, ([], n_color, n_distance))        
            results.append(new_entry)
            
        color = 'BLACK'   
    results.append((hero_id, (friend_list, color, distance)))
    return results
  
def bfs_reducer(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    color1 = data1[1]
    color2 = data2[1]
    distance1 = data1[2]
    distance2 = data2[2]
    
    distance = 9999
    color = 'WHITE'
    edges = []
    
    if len(edges1) > 0:
        edges = edges1
    elif len(edges2) > 0:
        edges = edges2

    if distance1 < distance2:
        distance = distance1
    elif distance2 < distance:
        distance = distance2
    
    if color1 != 'WHITE' and color2 == 'WHITE':
        color = color1
    elif color1 == 'WHITE' and color2 != 'WHITE':
        color = color2
    elif color1 == 'GRAY' and color2 == 'GRAY':
        color = 'GRAY'
    elif color1 == 'BLACK' and color2 == 'BLACK':
        color = 'BLACK'
    
    return (edges, color, distance)

iterationRDD = create_starting_rdd()

for i in range(10):
    print "***********RUNNING BFS ITERATION: %d *************" %i
    
    mapped = iterationRDD.flatMap(bfs_mapper)
    print "Mapped Count", mapped.count()    
    if  hitcounter.value > 0:
        print "Hit the target charactor from", hitcounter.value, "locations"
        break
    
    iterationRDD = mapped.reduceByKey(bfs_reducer)    
    
#print superhero_rdd.collect()

